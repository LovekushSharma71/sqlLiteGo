package LinearTable

import (
	"bytes"
	"db/DiskManager"
	"encoding/binary"
	"fmt"
)

type Table struct {
	Table *DiskManager.DiskManager
}

type Node struct {
	Key int64
	Val string
}

func InitLinearTable(fileName string) (*Table, error) {

	dsk, err := DiskManager.InitDiskManager(fileName)
	if err != nil {
		return nil, err
	}
	return &Table{
		Table: dsk,
	}, nil
}

func (t *Table) Close() {
	t.Table.Close()
}

func (t *Table) Select() {

	srt := t.Table.SrtOffset
	for {
		if srt >= t.Table.EndOffset {
			break
		}
		pge, err := t.Table.GetDiskData(int64(srt))
		if err != nil {
			panic(err)
		}
		nde, err := DeserializeNode(pge.Data.([]byte))
		if err != nil {
			panic(err)
		}
		fmt.Printf("key: %d , val: %s\n", nde.Key, nde.Val)
		srt += DiskManager.DskAddr(DiskManager.HEADER_SIZE + pge.Header.Size)
	}
}

func (t *Table) Insert(k int64, v string) error {

	n := Node{Key: k, Val: v}
	buf, err := SerializeNode(n)
	if err != nil {
		return err
	}
	t.Table.WrtDiskData(&DiskManager.DiskData{
		Header: DiskManager.RecordHeader{
			Type: DiskManager.DT_BYTES,
		},
		Data: buf,
	})
	return nil
}

func SerializeNode(n Node) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, DiskManager.BINARY_ORDER, n.Key)
	if err != nil {
		return nil, fmt.Errorf("error writing key: %w", err)
	}

	valueBytes := []byte(n.Val)
	err = binary.Write(&buf, DiskManager.BINARY_ORDER, int32(len(valueBytes)))
	if err != nil {
		return nil, fmt.Errorf("error writing value length: %w", err)
	}

	_, err = buf.Write(valueBytes)
	if err != nil {
		return nil, fmt.Errorf("error writing value: %w", err)
	}

	return buf.Bytes(), nil
}

func DeserializeNode(data []byte) (Node, error) {
	var node Node
	buf := bytes.NewReader(data)

	err := binary.Read(buf, DiskManager.BINARY_ORDER, &node.Key)
	if err != nil {
		return Node{}, fmt.Errorf("error reading key: %w", err)
	}

	var valueLen int32
	err = binary.Read(buf, DiskManager.BINARY_ORDER, &valueLen)
	if err != nil {
		return Node{}, fmt.Errorf("error reading value length: %w", err)
	}

	valueBytes := make([]byte, valueLen)
	_, err = buf.Read(valueBytes)
	if err != nil {
		return Node{}, fmt.Errorf("error reading value: %w", err)
	}
	node.Val = string(valueBytes)

	return node, nil
}
