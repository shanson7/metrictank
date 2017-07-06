package memory

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	// metric idx.memory.update is the number of updates to the memory idx
	statUpdate = stats.NewCounter32("idx.memory.ops.update")
	// metric idx.memory.add is the number of additions to the memory idx
	statAdd = stats.NewCounter32("idx.memory.ops.add")
	// metric idx.memory.add is the duration of a (successful) add of a metric to the memory idx
	statAddDuration = stats.NewLatencyHistogram15s32("idx.memory.add")
	// metric idx.memory.update is the duration of (successful) update of a metric to the memory idx
	statUpdateDuration = stats.NewLatencyHistogram15s32("idx.memory.update")
	// metric idx.memory.get is the duration of a get of one metric in the memory idx
	statGetDuration = stats.NewLatencyHistogram15s32("idx.memory.get")
	// metric idx.memory.list is the duration of memory idx listings
	statListDuration = stats.NewLatencyHistogram15s32("idx.memory.list")
	// metric idx.memory.find is the duration of memory idx find
	statFindDuration = stats.NewLatencyHistogram15s32("idx.memory.find")
	// metric idx.memory.delete is the duration of a delete of one or more metrics from the memory idx
	statDeleteDuration = stats.NewLatencyHistogram15s32("idx.memory.delete")
	// metric idx.memory.prune is the duration of successful memory idx prunes
	statPruneDuration = stats.NewLatencyHistogram15s32("idx.memory.prune")

	// metric idx.memory.filtered is number of series that have been excluded from responses due to their lastUpdate property
	statFiltered = stats.NewCounter32("idx.memory.filtered")

	// metric idx.metrics_active is the number of currently known metrics in the index
	statMetricsActive = stats.NewGauge32("idx.metrics_active")

	Enabled bool
)

func ConfigSetup() {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	globalconf.Register("memory-idx", memoryIdx)
}

type Tree struct {
	Items map[string]*Node // key is the full path of the node.
}

type Node struct {
	Name     string
	Parent   *Node
	Children *Tree
	Defs     []string
}

func NewNode(name string, parent *Node) *Node {
	return &Node{
		Name:     name,
		Parent:   parent,
		Children: &Tree{Items: make(map[string]*Node)},
		Defs:     make([]string, 0),
	}
}
func (n *Node) HasChildren() bool {
	return len(n.Children.Items) > 0
}

func (n *Node) Leaf() bool {
	return len(n.Defs) > 0
}

func (n *Node) List() []*Node {
	results := make([]*Node, 0)
	if n.Leaf() {
		results = append(results, n)
	}

	for _, c := range n.Children.Items {
		results = append(results, c.List()...)
	}
	return results
}

func (n *Node) Path() string {
	// Work our way up to build up the path
	pathStrings := []string{n.Name}
	parent := n.Parent
	for parent != nil {
		pathStrings = append(pathStrings, parent.Name)
		parent = parent.Parent
	}

	// In-place reverse
	for i := len(pathStrings)/2 - 1; i >= 0; i-- {
		opp := len(pathStrings) - 1 - i
		pathStrings[i], pathStrings[opp] = pathStrings[opp], pathStrings[i]
	}
	return strings.Join(pathStrings, ".")
}

func (n *Node) String() string {
	if n.Leaf() {
		return fmt.Sprintf("leaf - %s", n.Path())
	}
	return fmt.Sprintf("branch - %s", n.Path())
}

type NodePath struct {
	Path string
	Node *Node
}

type NodeArchive struct {
	Archive *idx.Archive // Note that the "Name" will have been removed from here and can be rebuilt from "Node". "Metric is removed as well...why have both again?"
	Node    *Node
}

func NewNodeArchive(archive *idx.Archive, node *Node) NodeArchive {
	newArchive := *archive
	newArchive.MetricDefinition.Name = ""
	newArchive.MetricDefinition.Metric = ""
	return NodeArchive{
		Archive: &newArchive,
		Node:    node,
	}
}

func (na *NodeArchive) getFullArchive() idx.Archive {
	result := *na.Archive
	path := na.Node.Path()
	result.MetricDefinition.Name = path
	result.MetricDefinition.Metric = path
	return result
}

// Implements the the "MetricIndex" interface
type MemoryIdx struct {
	sync.RWMutex
	DefById map[string]*NodeArchive
	Tree    map[int]*Node
}

func New() *MemoryIdx {
	return &MemoryIdx{
		DefById: make(map[string]*NodeArchive),
		Tree:    make(map[int]*Node),
	}
}

func (m *MemoryIdx) Init() error {
	return nil
}

func (m *MemoryIdx) Stop() {
	return
}

func (m *MemoryIdx) AddOrUpdate(data *schema.MetricData, partition int32) idx.Archive {
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	existing, ok := m.DefById[data.Id]
	if ok {
		log.Debug("metricDef with id %s already in index.", data.Id)
		existing.Archive.LastUpdate = data.Time
		existing.Archive.Partition = partition
		statUpdate.Inc()
		statUpdateDuration.Value(time.Since(pre))

		return existing.getFullArchive()
	}

	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	archive := m.add(def)
	statMetricsActive.Inc()
	statAddDuration.Value(time.Since(pre))
	return archive
}

func (m *MemoryIdx) Update(entry idx.Archive) {
	m.Lock()
	if _, ok := m.DefById[entry.Id]; !ok {
		m.Unlock()
		return
	}
	nodeArchive := m.DefById[entry.Id]
	*(nodeArchive.Archive) = entry
	nodeArchive.Archive.MetricDefinition.Name = ""
	nodeArchive.Archive.MetricDefinition.Metric = ""

	m.Unlock()
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (m *MemoryIdx) Load(defs []schema.MetricDefinition) int {
	m.Lock()
	var pre time.Time
	var num int
	for i := range defs {
		def := &defs[i]
		pre = time.Now()
		if _, ok := m.DefById[def.Id]; ok {
			continue
		}
		m.add(def)
		// as we are loading the metricDefs from a persistent store, set the lastSave
		// to the lastUpdate timestamp.  This wont exactly match the true lastSave Timstamp,
		// but it will be close enough and it will always be true that the lastSave was at
		// or after this time.  For metrics that are sent at or close to real time (the typical
		// use case), then the value will be within a couple of seconds of the true lastSave.
		m.DefById[def.Id].Archive.LastSave = uint32(def.LastUpdate)
		num++
		statMetricsActive.Inc()
		statAddDuration.Value(time.Since(pre))
	}
	m.Unlock()
	return num
}

func (m *MemoryIdx) add(def *schema.MetricDefinition) idx.Archive {
	path := def.Name
	schemaId, _ := mdata.MatchSchema(def.Name, def.Interval)
	aggId, _ := mdata.MatchAgg(def.Name)
	archive := &idx.Archive{
		MetricDefinition: *def,
		SchemaId:         schemaId,
		AggId:            aggId,
	}

	//first check to see if a tree has been created for this OrgId
	root, ok := m.Tree[def.OrgId]
	if !ok || len(root.Children.Items) == 0 {
		log.Debug("memory-idx: first metricDef seen for orgId %d", def.OrgId)
		m.Tree[def.OrgId] = NewNode("", nil)
		root = m.Tree[def.OrgId]
	}

	// Dig into the tree, building it as needed
	nodes := strings.Split(path, ".")

	currentNode := root
	for _, node := range nodes {
		next, exists := currentNode.Children.Items[node]
		if !exists {
			log.Debug("memory-idx: Adding %s as child of %s", node, currentNode.Name)
			copiedName := string([]byte(node))
			currentNode.Children.Items[copiedName] = NewNode(copiedName, currentNode)
			next = currentNode.Children.Items[copiedName]
		}
		currentNode = next
	}

	currentNode.Defs = append(currentNode.Defs, def.Id)
	newNodeArchive := NewNodeArchive(archive, currentNode)
	m.DefById[def.Id] = &newNodeArchive
	statAdd.Inc()
	return *archive
}

func (m *MemoryIdx) Get(id string) (idx.Archive, bool) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	def, ok := m.DefById[id]
	statGetDuration.Value(time.Since(pre))
	if ok {
		return def.getFullArchive(), ok
	}
	return idx.Archive{}, ok
}

// GetPath returns the node under the given org and path.
// this is an alternative to Find for when you have a path, not a pattern, and want to lookup in a specific org tree only.
func (m *MemoryIdx) GetPath(orgId int, path string) []idx.Archive {
	m.RLock()
	defer m.RUnlock()
	root, ok := m.Tree[orgId]
	if !ok {
		return nil
	}

	// Dig into the tree
	nodes := strings.Split(path, ".")

	currentNode := root
	for _, node := range nodes {
		next, exists := currentNode.Children.Items[node]
		if !exists {
			return nil
		}
		currentNode = next
	}

	archives := make([]idx.Archive, len(currentNode.Defs))
	for i, def := range currentNode.Defs {
		archive := m.DefById[def]
		archives[i] = archive.getFullArchive()
	}
	return archives
}

func (m *MemoryIdx) Find(orgId int, pattern string, from int64) ([]idx.Node, error) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	matchedNodes, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}
	publicNodes, err := m.find(-1, pattern)
	if err != nil {
		return nil, err
	}
	matchedNodes = append(matchedNodes, publicNodes...)
	log.Debug("memory-idx: %d nodes matching pattern %s found", len(matchedNodes), pattern)
	results := make([]idx.Node, 0)
	seen := make(map[string]struct{})
	// if there are public (orgId -1) and private leaf nodes with the same series
	// path, then the public metricDefs will be excluded.
	for _, n := range matchedNodes {
		if _, ok := seen[n.Path]; !ok {
			idxNode := idx.Node{
				Path:        n.Path,
				Leaf:        n.Node.Leaf(),
				HasChildren: n.Node.HasChildren(),
			}
			if idxNode.Leaf {
				idxNode.Defs = make([]idx.Archive, 0, len(n.Node.Defs))
				for _, id := range n.Node.Defs {
					def := m.DefById[id]
					if from != 0 && def.Archive.LastUpdate < from {
						statFiltered.Inc()
						log.Debug("memory-idx: from is %d, so skipping %s which has LastUpdate %d", from, def.Archive.Id, def.Archive.LastUpdate)
						continue
					}
					log.Debug("memory-idx Find: adding to path %s archive id=%s name=%s int=%d schemaId=%d aggId=%d lastSave=%d", n.Path, def.Archive.Id, def.Node.Path(), def.Archive.Interval, def.Archive.SchemaId, def.Archive.AggId, def.Archive.LastSave)
					idxNode.Defs = append(idxNode.Defs, *def.Archive)
				}
				if len(idxNode.Defs) == 0 {
					continue
				}
			}
			results = append(results, idxNode)
			seen[n.Path] = struct{}{}
		} else {
			log.Debug("memory-idx: path %s already seen", n.Path)
		}
	}
	log.Debug("memory-idx: %d nodes has %d unique paths.", len(matchedNodes), len(results))
	statFindDuration.Value(time.Since(pre))
	return results, nil
}

func (m *MemoryIdx) find(orgId int, pattern string) ([]NodePath, error) {
	var results []NodePath
	root, ok := m.Tree[orgId]
	if !ok {
		log.Debug("memory-idx: orgId %d has no metrics indexed.", orgId)
		return results, nil
	}

	nodes := strings.Split(pattern, ".")

	children := []NodePath{NodePath{"", root}}
	for _, node := range nodes {
		grandChildren := make([]NodePath, 0)
		for _, c := range children {
			if !c.Node.HasChildren() {
				log.Debug("memory-idx: end of branch reached at %s with no match found for %s", c.Path, pattern)
				// expecting a branch
				continue
			}
			log.Debug("memory-idx: searching %d children of %s that match %s", len(c.Node.Children.Items), c.Path, node)
			matches, err := match(node, c.Node.Children)
			if err != nil {
				return results, err
			}
			for _, m := range matches {
				newPath := c.Path + "." + m.Name
				if c.Path == "" {
					newPath = m.Name
				}
				grandChildren = append(grandChildren, NodePath{newPath, m})
			}
		}
		children = grandChildren
		if len(children) == 0 {
			log.Debug("memory-idx: pattern does not match any series.")
			break
		}
	}

	for _, c := range children {
		results = append(results, c)
	}

	return results, nil
}

func match(pattern string, candidates *Tree) ([]*Node, error) {
	var patterns []string
	if strings.ContainsAny(pattern, "{}") {
		patterns = expandQueries(pattern)
	} else {
		patterns = []string{pattern}
	}

	results := make([]*Node, 0)
	for _, p := range patterns {
		if strings.ContainsAny(p, "*[]?") {
			p = strings.Replace(p, "*", ".*", -1)
			p = strings.Replace(p, "?", ".?", -1)
			p = "^" + p + "$"
			r, err := regexp.Compile(p)
			if err != nil {
				log.Debug("memory-idx: regexp failed to compile. %s - %s", p, err)
				return nil, err
			}
			for _, c := range candidates.Items {
				if r.MatchString(c.Name) {
					log.Debug("memory-idx: %s matches %s", c.Name, p)
					results = append(results, c)
				}
			}
		} else {
			c, ok := candidates.Items[p]
			if ok {
				log.Debug("memory-idx: %s is exact match", c.Name)
				results = append(results, c)
			}
		}
	}
	return results, nil
}

func (m *MemoryIdx) List(orgId int) []idx.Archive {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	orgs := []int{-1, orgId}
	if orgId == -1 {
		log.Info("memory-idx: returning all metricDefs for all orgs")
		orgs = make([]int, len(m.Tree))
		i := 0
		for org := range m.Tree {
			orgs[i] = org
			i++
		}
	}
	defs := make([]idx.Archive, 0)
	for _, org := range orgs {
		root, ok := m.Tree[org]
		if !ok {
			continue
		}
		leafs := root.List()
		for _, n := range leafs {
			if !n.Leaf() {
				log.Debug("memory-idx - Got non-leaf result from Node.List() of %s", n.Path())
				continue
			}
			for _, id := range n.Defs {
				defs = append(defs, *m.DefById[id].Archive)
			}
		}
	}
	statListDuration.Value(time.Since(pre))

	return defs
}

func (m *MemoryIdx) Delete(orgId int, pattern string) ([]idx.Archive, error) {
	var deletedDefs []idx.Archive
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	found, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}

	for _, f := range found {
		deleted := m.delete(orgId, f.Node, true)
		statMetricsActive.DecUint32(uint32(len(deleted)))
		deletedDefs = append(deletedDefs, deleted...)
	}
	statDeleteDuration.Value(time.Since(pre))
	return deletedDefs, nil
}

func (m *MemoryIdx) delete(orgId int, n *Node, deleteEmptyParents bool) []idx.Archive {
	deletedDefs := make([]idx.Archive, 0)

	// Delete any children
	for _, child := range n.Children.Items {
		deleted := m.delete(orgId, child, false)
		deletedDefs = append(deletedDefs, deleted...)
	}

	// delete the metricDefs
	for _, id := range n.Defs {
		log.Debug("memory-idx: deleting %s from index", id)
		deletedDefs = append(deletedDefs, *m.DefById[id].Archive)
		delete(m.DefById, id)
	}

	// delete the node from the parent.
	delete(n.Parent.Children.Items, n.Name)

	if !deleteEmptyParents {
		return deletedDefs
	}

	// Walk up the parent structure removing empty parents until we hit a non-empty parent.
	parent := n.Parent
	for parent != nil && parent.Parent != nil && len(parent.Children.Items) == 0 {
		delete(parent.Parent.Children.Items, parent.Name)
		parent = parent.Parent
	}

	return deletedDefs
}

// delete series from the index if they have not been seen since "oldest"
func (m *MemoryIdx) Prune(orgId int, oldest time.Time) ([]idx.Archive, error) {
	oldestUnix := oldest.Unix()
	var pruned []idx.Archive
	pre := time.Now()
	m.RLock()
	orgs := []int{orgId}
	if orgId == -1 {
		log.Info("memory-idx: pruning stale metricDefs across all orgs")
		orgs = make([]int, len(m.Tree))
		i := 0
		for org := range m.Tree {
			orgs[i] = org
			i++
		}
	}
	m.RUnlock()
	for _, org := range orgs {
		m.Lock()
		root, ok := m.Tree[org]
		if !ok {
			m.Unlock()
			continue
		}

		leafs := root.List()

		for _, n := range leafs {
			staleCount := 0
			for _, id := range n.Defs {
				if m.DefById[id].Archive.LastUpdate < oldestUnix {
					staleCount++
				}
			}
			if staleCount == len(n.Defs) {
				log.Debug("memory-idx: series %s for orgId:%d is stale. pruning it.", n.Path(), org)
				//we need to delete this node.
				defs := m.delete(org, n, true)
				statMetricsActive.Dec()
				pruned = append(pruned, defs...)
			}
		}
		m.Unlock()
	}
	if orgId == -1 {
		log.Info("memory-idx: pruning stale metricDefs from memory for all orgs took %s", time.Since(pre).String())
	}
	statPruneDuration.Value(time.Since(pre))
	return pruned, nil
}

// filepath.Match doesn't support {} because that's not posix, it's a bashism
// the easiest way of implementing this extra feature is just expanding single queries
// that contain these queries into multiple queries, who will be checked separately
// and whose results will be ORed.
func expandQueries(query string) []string {
	queries := []string{query}

	// as long as we find a { followed by a }, split it up into subqueries, and process
	// all queries again
	// we only stop once there are no more queries that still have {..} in them
	keepLooking := true
	for keepLooking {
		expanded := make([]string, 0)
		keepLooking = false
		for _, query := range queries {
			lbrace := strings.Index(query, "{")
			rbrace := -1
			if lbrace > -1 {
				rbrace = strings.Index(query[lbrace:], "}")
				if rbrace > -1 {
					rbrace += lbrace
				}
			}

			if lbrace > -1 && rbrace > -1 {
				keepLooking = true
				expansion := query[lbrace+1 : rbrace]
				options := strings.Split(expansion, ",")
				for _, option := range options {
					expanded = append(expanded, query[:lbrace]+option+query[rbrace+1:])
				}
			} else {
				expanded = append(expanded, query)
			}
		}
		queries = expanded
	}
	return queries
}
