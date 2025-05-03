package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	DataFile string = "Data/TRoot"
	MinKeys  int    = 1
	MaxKeys  int    = 3
	// i.e. MinKeys<=num_keys<MaxKeys
)

var BINARY_ORDER binary.ByteOrder = binary.LittleEndian
var PG_SIZE int32 = int32(binary.Size(Page{}))

type PageId int32

type Disk struct {
	File *os.File
}

type Page struct {
	IsLeaf   bool
	NodeCnt  int32
	Nodes    [MaxKeys]Node
	Children [MaxKeys + 1]PageId
}

type Node struct {
	Key int32
	Val [255]byte
}

// returns index if found else give index of element just greater than key
func Search(n []Node, key int32) int {
	var i, j int = 0, len(n) - 1
	var m, cand int
	for i <= j {
		m = i + (j-i)/2
		if n[m].Key == key {
			return m
		} else if n[m].Key < key {
			i = m + 1
			cand = m
		} else {
			j = m - 1
		}
	}
	return cand
}

func InitDisk() *Disk {

	file, err := os.OpenFile(DataFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	return &Disk{
		File: file,
	}
}

func (dsk *Disk) Close() {

	err := dsk.File.Close()
	if err != nil {
		panic(err)
	}
}

func (dsk *Disk) getPage(pid PageId) (*Page, error) {

	buf := make([]byte, PG_SIZE)
	n, err := dsk.File.ReadAt(buf, int64(pid*PageId(PG_SIZE)))
	fmt.Println("bytes read", n, "pg size", PG_SIZE)
	if err != nil {
		return nil, err
	}
	if n < int(PG_SIZE) {
		return nil, fmt.Errorf("expected %d bytes for page, but only read %d bytes", PG_SIZE, n)
	}

	rd := bytes.NewReader(buf)
	pge := Page{}
	if err := binary.Read(rd, BINARY_ORDER, &pge); err != nil {
		return nil, err
	}
	return &pge, nil
}

func (dsk *Disk) wrtPage(pid PageId, pge *Page) error {

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, pge); err != nil {
		return err
	}

	offset := int64(pid) * int64(PG_SIZE)
	_, err := dsk.File.WriteAt(buf.Bytes(), offset)
	if err != nil {
		return err
	}

	return nil
}

func (table *Disk) Select(root *Page, key int32) (*Node, error) {

	ind := Search(root.Nodes[:], key)
	if root.Nodes[ind].Key == key {
		return &root.Nodes[ind], nil
	} else if root.IsLeaf {
		return nil, fmt.Errorf("key not found")
	} else {
		var page *Page
		var err error
		if ind == 0 && root.Nodes[ind].Key > key {
			page, err = table.getPage(root.Children[0])
			if err != nil {
				return nil, err
			}
		} else {
			page, err = table.getPage(root.Children[ind+1])
			if err != nil {
				return nil, err
			}
		}

		node, err := table.Select(page, key)
		if err != nil {
			return nil, err
		}
		return node, nil
	}

}
func (table *Disk) Insert(root *Page, key int32, val string) error {

	ind := Search(root.Nodes[:], key)
	if root.Nodes[ind].Key == key {
		return fmt.Errorf("duplicate keys are not allowed: key %d does not exist", ind)
	}
	if root.IsLeaf {

	}
	if ind == 0 && root.Nodes[0].Key > key {
		table.getPage(root.Children[ind])
	}
	page, err := table.getPage(root.Children[ind+1])
	if err != nil {
		return err
	}
	table.Insert(page, key, val)

	return nil
}

// func (pge *Page) Update() {

// }
// func (pge *Page) Delete() {

// }
