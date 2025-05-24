package diskmanager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"sync"
)

func InitDiskManager(fileName string) (*DiskManager, error) {

	file, err := os.OpenFile(DB_FILE+fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("InitDiskManger error, in opening file %s: %w", fileName, err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("InitDiskManager error: %w", err)
	}
	size := info.Size()

	var isTree bool
	if fileName == TREE_FILE {
		isTree = true
	} else {
		isTree = false
	}

	return &DiskManager{
		FilObj: file,
		SrtOff: 0,
		Cursor: 0,
		EndOff: int32(size),
		IsTree: isTree,
		MuLock: sync.Mutex{},
	}, nil
}

// TODO: resolve deserialisation error
func (d *DiskManager) GetDiskData() (*DiskData, error) {

	var buf []byte
	if d.IsTree {
		buf = make([]byte, TREE_DISKDATA_SIZE)
	} else {
		buf = make([]byte, LINEAR_DISKDATA_SIZE)
	}

	n, err := d.FilObj.ReadAt(buf, int64(d.Cursor))
	if err != nil {
		return nil, fmt.Errorf("GetDiskData error, read error: %w", err)
	}
	if n != len(buf) {
		return nil, fmt.Errorf("GetDiskData error, invalid length read: expected len %d got %d", len(buf), n)
	}

	data, err := DeserializeDskData(buf)
	if err != nil {
		return nil, fmt.Errorf("GetDiskData error, deserialization error: %w", err)
	}
	return data, nil
}

func (d *DiskManager) WrtDiskData(data interface{}) (*DiskData, error) {

	if data == nil {
		return nil, fmt.Errorf("WrtDiskData error: input cannot be nil")
	}

	dskData := &DiskData{
		RecHead: DskDataHdr{
			Deleted: false,
			RecAddr: d.EndOff,
		},
		RecData: data,
	}

	switch reflect.TypeOf(data) {
	case reflect.TypeOf(TreePage{}):
		dskData.RecHead.RecType = DT_TREE_PAGE
		dskData.RecHead.RecSize = int32(LINEAR_PAGE_SIZE)
	case reflect.TypeOf(ListPage{}):
		dskData.RecHead.RecType = DT_LIST_PAGE
		dskData.RecHead.RecSize = int32(TREE_PAGE_SIZE)
	default:
		return nil, fmt.Errorf("WrtDiskData error: data type %T not supported", data)
	}

	buf, err := SerializeDiskData(dskData)
	if err != nil {
		return nil, fmt.Errorf("WrtDiskData error: %s", err.Error())
	}
	_, err = d.FilObj.WriteAt(buf, int64(dskData.RecHead.RecAddr))
	if err != nil {
		return nil, fmt.Errorf("WrtDiskData error: %s", err.Error())
	}
	switch dskData.RecHead.RecType {
	case DT_LIST_PAGE:
		d.EndOff += int32(LINEAR_DISKDATA_SIZE)
	case DT_TREE_PAGE:
		d.EndOff += int32(TREE_DISKDATA_SIZE)
	}
	return dskData, nil
}

// assume that cursor is set before
func (d *DiskManager) EdtDiskData(data interface{}) error {

	if data == nil {
		return fmt.Errorf("EditDiskData error: input cannot be nil")
	}

	dskData := &DiskData{
		RecHead: DskDataHdr{
			Deleted: false,
			RecAddr: d.Cursor,
		},
		RecData: data,
	}

	switch reflect.TypeOf(data) {
	case reflect.TypeOf(TreePage{}):
		dskData.RecHead.RecType = DT_TREE_PAGE
		dskData.RecHead.RecSize = int32(TREE_PAGE_SIZE)
	case reflect.TypeOf(ListPage{}):
		dskData.RecHead.RecType = DT_LIST_PAGE
		dskData.RecHead.RecSize = int32(LINEAR_PAGE_SIZE)
	default:
		return fmt.Errorf("EdtDiskData error: data type %T not supported", data)
	}

	buf := make([]byte, HEADER_SIZE)
	_, err := d.FilObj.ReadAt(buf, int64(d.Cursor))
	if err != nil {
		return fmt.Errorf("EdtDiskData error: %s", err.Error())
	}

	reader := bytes.NewReader(buf)
	hdr := &DskDataHdr{}
	if err := binary.Read(reader, BINARY_ORDER, hdr); err != nil {
		return fmt.Errorf("EdtDiskData error: %s", err.Error())
	}

	if hdr.RecType != dskData.RecHead.RecType {
		return fmt.Errorf("EdtDiskData error, incompatible data type: expected %d given %d",
			hdr.RecType, dskData.RecHead.RecType)
	}

	buf, err = SerializeDiskData(dskData)
	if err != nil {
		return fmt.Errorf("EdtDiskData error: %s", err.Error())
	}

	_, err = d.FilObj.WriteAt(buf, int64(d.Cursor))
	if err != nil {
		return fmt.Errorf("EdtDiskData error: %s", err.Error())
	}

	return nil

}

func (d *DiskManager) DelDiskData() error {

	buf := make([]byte, HEADER_SIZE)
	_, err := d.FilObj.ReadAt(buf, int64(d.Cursor))
	if err != nil {
		return fmt.Errorf("DelDiskData error, reading full record for deletion: %w", err)
	}

	reader := bytes.NewReader(buf)
	hdr := &DskDataHdr{}
	if err := binary.Read(reader, BINARY_ORDER, hdr); err != nil {
		return fmt.Errorf("DelDiskData error: %s", err.Error())
	}

	switch hdr.RecType {
	case DT_LIST_PAGE:
		buf = make([]byte, LINEAR_DISKDATA_SIZE)
	case DT_TREE_PAGE:
		buf = make([]byte, TREE_DISKDATA_SIZE)
	default:
		return fmt.Errorf("DelDiskData error: invalid datatype stored in disk")
	}

	dskData, err := DeserializeDskData(buf)
	if err != nil {
		return fmt.Errorf("DelDiskData error: %s", err.Error())
	}
	dskData.RecHead.Deleted = true
	buf, err = SerializeDiskData(dskData)
	if err != nil {
		return fmt.Errorf("DelDiskData error: %s", err.Error())
	}

	_, err = d.FilObj.WriteAt(buf, int64(d.Cursor))
	if err != nil {
		return fmt.Errorf("DelDiskData error, write error: %s", err.Error())
	}
	return nil
}

func (d *DiskManager) WrtDiskHeader(head TableHeader) error {

	buf := new(bytes.Buffer)

	err := binary.Write(buf, BINARY_ORDER, head)
	if err != nil {
		return fmt.Errorf("WrtDiskHeader error: %s", err.Error())
	}
	_, err = d.FilObj.WriteAt(buf.Bytes(), 0)
	if err != nil {
		return fmt.Errorf("WrtDiskHeader error: %s", err.Error())
	}
	return nil
}

func (d *DiskManager) GetDiskHeader() (*TableHeader, error) {

	buf := make([]byte, TBL_HEAD_SIZE)

	n, err := d.FilObj.ReadAt(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("GetDiskHeader error: %s", err.Error())
	}
	if n != TBL_HEAD_SIZE {
		return nil, fmt.Errorf("GetDiskHeader error: invalid table head size in file")
	}

	var head *TableHeader
	reader := bytes.NewReader(buf)
	err = binary.Read(reader, BINARY_ORDER, head)
	if err != nil {
		return nil, fmt.Errorf("GetDiskHeader error: %s", err.Error())
	}
	return head, nil
}
