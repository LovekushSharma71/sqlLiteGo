package main

import (
	"fmt"
	"os"
)

const (
	PAGE_SIZE   = 4098
	ORDER       = 3 // max size=5 min size=2
	HEADER_SIZE = 64
	FILEPATH    = "Data/BTree"
)

type Header struct {
	RootOffset    int32
	Order         int32
	NxtFreeOffset int32
}

type BTree struct {
	File   *os.File
	Header Header
}

type Node struct {
	Offset  int32
	IsLeaf  bool
	IsRoot  bool // cause root can have 2 children at minimum
	NumKeys int32
	// since page size is fixed no need to fix array size
	Data     []Data  //  NumKeys
	Children []int32 // NumKeys+1
	Tree     *BTree  // reference data from tree and file
}

type Data struct {
	Key int32
	Val [4]byte
}

// Btree utils
func InitBTree() (*BTree, error) {

	file, err := os.OpenFile(FILEPATH, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	finfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("unable to get file info")
	}

	header := Header{}
	// Setting up header if not already exist
	if finfo.Size() < HEADER_SIZE {

		header.RootOffset = 0
		header.Order = ORDER
		header.NxtFreeOffset = 0
	}

	return &BTree{
		File: file,
	}, nil
}

// node utils
func NewNode(tree *BTree, isLeaf bool, isRoot bool) *Node {
	return &Node{
		Offset:   -1,
		IsLeaf:   isLeaf,
		IsRoot:   isRoot,
		NumKeys:  0,
		Data:     make([]Data, 2*ORDER-1),
		Children: make([]int32, 2*ORDER),
		Tree:     tree,
	}
}
