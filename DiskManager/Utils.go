package diskmanager

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func SerializeDiskData(data *DiskData) ([]byte, error) {

	if data == nil {
		return nil, fmt.Errorf("DiskData serialisation error: input DiskData cannot be nil")
	}

	buf := new(bytes.Buffer)

	if err := binary.Write(buf, BINARY_ORDER, data.RecHead); err != nil {
		return nil, fmt.Errorf("DiskData serialisation error, writing RecHead: %s", err.Error())
	}

	switch data.RecHead.RecType {
	case DT_LIST_PAGE:

		listPageData, ok := data.RecData.(ListPage)
		if !ok {
			return nil, fmt.Errorf("invalid RecData type: expected ListPage, got %T for RecType DT_LIST_PAGE", data.RecData)
		}

		if err := binary.Write(buf, BINARY_ORDER, listPageData); err != nil {
			return nil, fmt.Errorf("DiskData serialisation error, writing PageData(DT_LIST_PAGE): %s", err.Error())
		}

	case DT_TREE_PAGE:

		treePageData, ok := data.RecData.(TreePage)
		if !ok {
			return nil, fmt.Errorf("invalid RecData type: expected TreePage, got %T for RecType DT_TREE_PAGE", data.RecData)
		}

		if err := binary.Write(buf, BINARY_ORDER, treePageData); err != nil {
			return nil, fmt.Errorf("DiskData serialisation error, writing PageData(DT_TREE_PAGE): %s", err.Error())
		}

	default:
		return nil, fmt.Errorf("DiskData serialisation error: invalid data type")
	}

	return buf.Bytes(), nil
}

func DeserializeDskData(buf []byte) (*DiskData, error) {

	var data *DiskData = &DiskData{}
	reader := bytes.NewReader(buf)

	if err := binary.Read(reader, BINARY_ORDER, data.RecHead); err != nil {
		return nil, fmt.Errorf("DiskData deserialisation error, reading RecHead: %s", err.Error())
	}

	switch data.RecHead.RecType {
	case DT_LIST_PAGE:
		var listpge *ListPage = &ListPage{}
		if err := binary.Read(reader, BINARY_ORDER, listpge); err != nil {
			return nil, fmt.Errorf("DiskData deserialisation error, reading PageData(DT_LIST_PAGE): %s", err.Error())
		}

		data.RecData = listpge

	case DT_TREE_PAGE:
		var treepge *TreePage = &TreePage{}

		if err := binary.Read(reader, BINARY_ORDER, treepge); err != nil {
			return nil, fmt.Errorf("DiskData deserialisation error, reading PageData(DT_TREE_PAGE): %s", err.Error())
		}

		data.RecData = treepge
	default:
		return nil, fmt.Errorf("DiskData deserialisation error: invalid data type")
	}
	return data, nil
}
