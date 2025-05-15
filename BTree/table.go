package btree

import (
	"bytes"
	"db/DiskManager"
	"encoding/binary"
	"fmt"
)

const (
	BTREE_ORDER    = 4
	MIN_NODES_ROOT = 1
	MAX_NODES_ROOT = 3
	MIN_NODES      = 1
	MAX_NODES      = 3 
	// MIN_NODES<nodes<=MAX_NODES
)

type Table struct {
	Table *DiskManager.DiskManager
}

type Header struct {
	IsLeaf bool
	IsRoot bool
	NumNds int32
	Parent DiskManager.DskAddr
}

type Page struct {
	PgHeader Header
	PageData []Node
	Children []DiskManager.DskAddr
}

type Node struct {
	Len int64
	Key int64
	Val string
}

func InitBTree(fileName string) (*Table, error) {
	dsk, err := DiskManager.InitDiskManager(fileName)
	if err != nil {
		return nil, err
	}
	return &Table{
		Table: dsk,
	}, nil
}

func (t *Table) Select(root *Page) {

}

func (t *Table) Insert(root *Page,key int64, val string) {

	

}

func DeserializePage(data []byte) (*Page, error) {

	var pge *Page = &Page{}
	buf := bytes.NewReader(data)

	// deserialise header
	err := binary.Read(buf, DiskManager.BINARY_ORDER, pge.PgHeader.IsLeaf)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, DiskManager.BINARY_ORDER, pge.PgHeader.IsRoot)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, DiskManager.BINARY_ORDER, pge.PgHeader.NumNds)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, DiskManager.BINARY_ORDER, pge.PgHeader.Parent)
	if err != nil {
		return nil, err
	}

	// Serialize node
	pge.PageData = make([]Node, pge.PgHeader.NumNds)
	for i := 0; i < int(pge.PgHeader.NumNds); i++ {
		err = binary.Read(buf, DiskManager.BINARY_ORDER, pge.PageData[i].Len)
		if err != nil {
			return nil, err
		}
		err = binary.Read(buf, DiskManager.BINARY_ORDER, pge.PageData[i].Key)
		if err != nil {
			return nil, err
		}
		tmp := make([]byte, pge.PageData[i].Len)
		err = binary.Read(buf, DiskManager.BINARY_ORDER, tmp)
		if err != nil {
			return nil, err
		}
		pge.PageData[i].Val = string(tmp)
	}

	pge.Children = make([]DiskManager.DskAddr, pge.PgHeader.NumNds+1)
	err = binary.Read(buf, DiskManager.BINARY_ORDER, pge.Children)
	if err != nil {
		return nil, err
	}

	return pge, nil
}

func SerializePage(pge *Page) ([]byte, error) {
	if pge==nil{
		return nil,fmt.Errorf("nil page error")
	}

	var buf bytes.Buffer

	// serialize header
	err := binary.Write(&buf, DiskManager.BINARY_ORDER, pge.PgHeader.IsLeaf)
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, DiskManager.BINARY_ORDER, pge.PgHeader.IsRoot)
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, DiskManager.BINARY_ORDER, pge.PgHeader.NumNds)
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, DiskManager.BINARY_ORDER, pge.PgHeader.Parent)
	if err != nil {
		return nil, err
	}

	// serialise pagedata
	for _, v := range pge.PageData {
		err = binary.Write(&buf, DiskManager.BINARY_ORDER, v.Len)
		if err != nil {
			return nil, err
		}
		err = binary.Write(&buf, DiskManager.BINARY_ORDER, v.Key)
		if err != nil {
			return nil, err
		}
		tmp := []byte(v.Val)
		err = binary.Write(&buf, DiskManager.BINARY_ORDER, tmp)
		if err != nil {
			return nil, err
		}
	}

	// serialize children
	for _, v := range pge.Children {

		err = binary.Write(&buf, DiskManager.BINARY_ORDER, v)
		if err != nil {
			return nil, err
		}

	}

	return buf.Bytes(), nil
}
