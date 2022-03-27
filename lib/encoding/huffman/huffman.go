package huffman

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/utils"
	"sort"
	"strconv"
	"sync"
)

// ValueType is the type of the value stored in a Node.
type ValueType uint8

// Node in the Huffman tree.
type Node struct {
	Parent *Node     // Optional parent node, for fast code read-out
	Left   *Node     // Optional left node
	Right  *Node     // Optional right node
	Count  int       // Relative frequency
	Value  ValueType // Optional value, set if this is a leaf
}

// SortNodes implements sort.Interface, order defined by Node.Count.
type SortNodes []*Node

func (sn SortNodes) Len() int           { return len(sn) }
func (sn SortNodes) Less(i, j int) bool { return sn[i].Count < sn[j].Count }
func (sn SortNodes) Swap(i, j int)      { sn[i], sn[j] = sn[j], sn[i] }

// Code returns the Huffman code of the node.
// Left children get bit 0, Right children get bit 1.
// Implementation uses Node.Parent to walk "up" in the tree.
func (n *Node) Code() *CodeNode {
	c := &CodeNode{code: 0, length: 0}
	for parent := n.Parent; parent != nil; n, parent = parent, parent.Parent {
		if parent.Right == n { // bit 1
			c.code |= 1 << c.length
		} // else bit 0 => nothing to do with r
		c.length++
	}
	return c
}

// Build builds a Huffman tree from the specified leaves.
// The content of the passed slice is modified, if this is unwanted, pass a copy.
// Guaranteed that the same input slice will result in the same Huffman tree.
func Build(leaves []*Node) *Node {
	// We sort once and use binary insertion later on
	sort.Stable(SortNodes(leaves)) // Note: stable sort for deterministic output!

	return BuildSorted(leaves)
}

// BuildSorted builds a Huffman tree from the specified leaves which must be sorted by Node.Count.
// The content of the passed slice is modified, if this is unwanted, pass a copy.
// Guaranteed that the same input slice will result in the same Huffman tree.
func BuildSorted(leaves []*Node) *Node {
	if len(leaves) == 0 {
		return nil
	}

	for len(leaves) > 1 {
		left, right := leaves[0], leaves[1]
		parentCount := left.Count + right.Count
		parent := &Node{Left: left, Right: right, Count: parentCount}
		left.Parent = parent
		right.Parent = parent

		// Where to insert parent in order to remain sorted?
		ls := leaves[2:]
		idx := sort.Search(len(ls), func(i int) bool { return ls[i].Count >= parentCount })
		idx += 2

		// Insert
		copy(leaves[1:], leaves[2:idx])
		leaves[idx-1] = parent
		leaves = leaves[1:]
	}

	return leaves[0]
}

// Print traverses the Huffman tree and prints the values with their code in binary representation.
// For debugging purposes.
func Print(root *Node) {
	// traverse traverses a subtree from the given node,
	// using the prefix code leading to this node, having the number of bits specified.
	var traverse func(n *Node, code uint64, bits byte)

	traverse = func(n *Node, code uint64, bits byte) {
		if n.Left == nil {
			// Leaf
			fmt.Printf("'%c': %0"+strconv.Itoa(int(bits))+"b\n", n.Value, code)
			return
		}
		bits++
		traverse(n.Left, code<<1, bits)
		traverse(n.Right, code<<1+1, bits)
	}

	traverse(root, 0, 0)
}

type CodeNode struct {
	code   uint16
	length int
}

type HuffmanCompressor struct {
	leaves   []*Node
	valueMap map[ValueType]*CodeNode
	root     *Node
}

func newHuffmanCompressor() *HuffmanCompressor {
	leaves := []*Node{
		{Value: 0, Count: 4730086893},
		{Value: 1, Count: 140062372},
		{Value: 2, Count: 142505858},
		{Value: 3, Count: 887293580},
		{Value: 4, Count: 357317923},
		{Value: 5, Count: 276924180},
		{Value: 6, Count: 753344560},
		{Value: 7, Count: 95991367},
		{Value: 8, Count: 78135772},
		{Value: 9, Count: 813707073},
		{Value: 10, Count: 319745995},
		{Value: 11, Count: 67332811},
		{Value: 12, Count: 693685469},
		{Value: 13, Count: 111126662},
		{Value: 14, Count: 79943356},
		{Value: 15, Count: 300999249},
	}

	s := &HuffmanCompressor{
		valueMap: map[ValueType]*CodeNode{},
		leaves:   make([]*Node, 16),
	}

	copy(s.leaves, leaves)
	s.root = Build(leaves)
	for _, leaf := range s.leaves {
		s.valueMap[leaf.Value] = leaf.Code()
	}
	return s
}

func (h *HuffmanCompressor) Compress(dst []byte, src []int64, firstValue int64) []byte {
	// consider src does not contains firstValue
	bs := &utils.ByteWrapper{Stream: &dst, Count: 0}
	bs.AppendBits(uint64(len(src)), 16)
	prev := firstValue
	tmp := &CodeNode{}
	src = src[1:]
	for _, s := range src {
		prev ^= s
		for i := 60; i >= 0; i -= 4 {
			tmp = h.valueMap[ValueType(prev>>i)&0x0F]
			bs.AppendBits(uint64(tmp.code), tmp.length)
		}
		prev = s
	}
	bs.Finish()
	return dst
}

func (h *HuffmanCompressor) Decompress(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	bs := &utils.ByteWrapper{Stream: &src, Count: 8}
	length, err := bs.ReadBits(16)
	if err != nil {
		return nil, err
	}
	if int(length) < itemsCount-1 {
		return nil, errors.New(fmt.Sprintf("overflow itemsCount more than compressed items, max is %d but got %d\n", length, itemsCount))
	}
	dst = append(dst, firstValue)
	node := &Node{}
	var v int64
	var right utils.Bit
	prev := firstValue
	for i := 1; i < itemsCount; i++ {
		v = 0
		for j := 0; j < 16; j++ {
			node = h.root
			for node.Left != nil {
				if right, err = bs.ReadBit(); err != nil {
					return nil, err
				} else if right {
					node = node.Right
				} else {
					node = node.Left
				}
			}
			v <<= 4
			v |= int64(node.Value & 0xF)
		}
		prev ^= v
		dst = append(dst, prev)
	}
	return dst, nil
}

var huffmanCompressorPool sync.Pool

func GetHuff() *HuffmanCompressor {
	h := huffmanCompressorPool.Get()
	if h == nil {
		return newHuffmanCompressor()
	}
	return h.(*HuffmanCompressor)
}

func PutHuff(h *HuffmanCompressor) {
	huffmanCompressorPool.Put(h)
}
