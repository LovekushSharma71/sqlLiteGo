package btree

import (
	"bytes"
	"db/DiskManager"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	BTREE_ORDER = 4
	MIN_NODES   = 1
	MAX_NODES   = 3 
	// MIN_NODES<=nodes<=MAX_NODES
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

func (t *Table) Insert(root *Page, addr DiskManager.DskAddr, key int64, val string) (DiskManager.DskAddr, *Node, DiskManager.DskAddr, DiskManager.DskAddr, error) {

	if root.PgHeader.IsLeaf {

		if len(root.PageData) < MAX_NODES {

			nde := append(root.PageData, Node{Len: int64(len(val)), Key: key, Val: val})
			sort.Slice(nde, func(i, j int) bool {
				return nde[i].Key < nde[i].Key
			})

			pge := Page{
				PgHeader: Header{
					IsLeaf: root.PgHeader.IsLeaf,
					IsRoot: root.PgHeader.IsRoot,
					NumNds: root.PgHeader.NumNds + 1,
					Parent: root.PgHeader.Parent,
				},
				PageData: nde,
				Children: root.Children,
			}

			err := t.Table.EditHeader(addr, DiskManager.HD_STAT, false)
			if err != nil {
				return -1, nil, -1, -1, err
			}

			data, err := SerializePage(&pge)
			if err != nil {
				return -1, nil, -1, -1, err
			}
			ddata := DiskManager.DiskData{
				Header: DiskManager.RecordHeader{
					Type: DiskManager.DT_BYTES,
				},
				Data: data,
			}
			err = t.Table.WrtDiskData(&ddata)
			if err != nil {
				return -1, nil, -1, -1, err
			}

			return ddata.Header.Addr, nil, -1, -1, nil

		}

	}
	return -1, nil, -1, -1, nil
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
	if pge == nil {
		return nil, fmt.Errorf("nil page error")
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
