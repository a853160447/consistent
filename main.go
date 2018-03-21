package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

const DEFAULT_REPLICAS = 160

//HashRing -
type HashRing []uint32

//Len -
func (c HashRing) Len() int {
	return len(c)
}

//Less -大小
func (c HashRing) Less(i, j int) bool {
	return c[i] < c[j]
}

//Swap - 交换
func (c HashRing) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

//Node -
type Node struct {
	ID       int
	IP       string
	Port     int
	HostName string
	Weight   int
}

//NewNode -
func NewNode(id int, ip string, port int, name string, weight int) *Node {
	return &Node{
		ID:       id,
		IP:       ip,
		Port:     port,
		HostName: name,
		Weight:   weight,
	}
}

//Consistent -
type Consistent struct {
	Nodes     map[uint32]Node
	numReps   int
	Resources map[int]bool
	ring      HashRing
	sync.RWMutex
}

//NewConsistent -
func NewConsistent() *Consistent {
	return &Consistent{
		Nodes:     make(map[uint32]Node),
		numReps:   DEFAULT_REPLICAS,
		Resources: make(map[int]bool),
		ring:      HashRing{},
	}
}

//Add -
func (c *Consistent) Add(node *Node) bool {
	//互斥锁是传统的并发程序对共享资源进行访问控制的主要手段
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.ID]; ok {
		return false
	}

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, node)
		c.Nodes[c.hashStr(str)] = *(node)
	}
	c.Resources[node.ID] = true
	c.sortHashRing()
	return true
}

//hash排序
func (c *Consistent) sortHashRing() {
	c.ring = HashRing{}
	for k := range c.Nodes {
		c.ring = append(c.ring, k)
	}
	sort.Sort(c.ring)
}

//生成固定格式string
func (c *Consistent) joinStr(i int, node *Node) string {
	return node.IP + "*" + strconv.Itoa(node.Weight) +
		"-" + strconv.Itoa(i) +
		"-" + strconv.Itoa(node.ID)
}

// MurMurHash算法 :https://github.com/spaolacci/murmur3
func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

//Get -
func (c *Consistent) Get(key string) Node {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)

	return c.Nodes[c.ring[i]]
}

func (c *Consistent) search(hash uint32) int {

	i := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })

	// logs.Info("+%v", c.ring)
	// logs.Info("|i|%v|ring|%v|hash|%v|", i, c.ring[i], hash)

	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.ring) - 1
	}
}

//Remove -
func (c *Consistent) Remove(node *Node) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.ID]; !ok {
		return
	}

	delete(c.Resources, node.ID)

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, node)
		delete(c.Nodes, c.hashStr(str))
	}
	c.sortHashRing()
}

func main() {

	cHashRing := NewConsistent()

	for i := 0; i < 10; i++ {
		si := fmt.Sprintf("%d", i)
		cHashRing.Add(NewNode(i, "172.18.1."+si, 8080, "host_"+si, 1))
	}

	for k, v := range cHashRing.Nodes {
		fmt.Println("Hash:", k, " IP", v.IP)
	}

	ipMap := make(map[string]int, 0)
	for i := 0; i < 1000; i++ {
		si := fmt.Sprintf("key%d", i)
		k := cHashRing.Get(si)
		if _, ok := ipMap[k.IP]; ok {
			ipMap[k.IP]++
		} else {
			ipMap[k.IP] = 1
		}
	}

	for k, v := range ipMap {
		fmt.Println("Node IP:", k, " count:", v)
	}
}
