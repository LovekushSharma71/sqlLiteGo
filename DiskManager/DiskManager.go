package DiskManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const (
	DB_FILE     = "Data/"
	HEADER_SIZE = 18
	TB_HDR_SIZE = 9
)

const (
	DT_STRING = iota
	DT_BYTES
	DT_INT64
	DT_INT32
	DT_INT16
	DT_INT8
)

const (
	HD_STAT = iota
	HD_ADDR
	HD_SIZE
	HD_TYPE
)

const (
	Linear = iota
	B_Tree
)

var BINARY_ORDER = binary.BigEndian

type DskAddr int64 // int64 representation of disk address

type TableHeader struct {
	RootAddr DskAddr
	TbleType int8
}

type RecordHeader struct {
	Stat int8 // false=deleted true=not deleted
	Addr DskAddr
	Size int64
	Type int8
}

type DiskData struct {
	// Header
	Header RecordHeader
	// Data
	Data interface{}
}

type DiskManager struct {
	File      *os.File
	SrtOffset DskAddr
	EndOffset DskAddr
	mu        sync.Mutex // just in case
}

func InitDiskManager(fileName string) (*DiskManager, error) {

	file, err := os.OpenFile(DB_FILE+fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()

	return &DiskManager{
		File:      file,
		SrtOffset: 0,
		EndOffset: DskAddr(size),
		mu:        sync.Mutex{},
	}, nil

}

// maybe use it in future
func (d *DiskManager) GetTableDetails() (*int8, error) {

	var buf []byte = make([]byte, TB_HDR_SIZE)
	n, err := d.File.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != TB_HDR_SIZE {
		return nil, fmt.Errorf("error getting header: expected %d got %d", TB_HDR_SIZE, n)
	}
	var t *int8
	reader := bytes.NewReader(buf)
	if err := binary.Read(reader, BINARY_ORDER, d.SrtOffset); err != nil {
		return nil, fmt.Errorf("binary read failed: %v", err)
	}
	if err := binary.Read(reader, BINARY_ORDER, t); err != nil {
		return nil, fmt.Errorf("binary read failed: %v", err)
	}
	return t, nil
}

func (d *DiskManager) WrtTableDetails(strt DskAddr, ty int8) error {

	buf := new(bytes.Buffer)

	if err := binary.Write(buf, BINARY_ORDER, strt); err != nil {
		return fmt.Errorf("binary.Write failed: %v", err)
	}
	if err := binary.Write(buf, BINARY_ORDER, ty); err != nil {
		return fmt.Errorf("binary.Write failed: %v", err)
	}

	n, err := d.File.Write(buf.Bytes())

	if err != nil {
		return err
	}
	if n != TB_HDR_SIZE {
		return fmt.Errorf("error writing header: expected %d got %d", TB_HDR_SIZE, n)
	}

	return nil
}

func (d *DiskManager) Close() error {
	return d.File.Close()
}

func (d *DiskManager) EditHeader(addr DskAddr, hdr int, val interface{}) error {

	header, err := d.GetHeader(addr)
	if err != nil {
		return err
	}
	var buf []byte

	switch hdr {
	case HD_STAT:

		v, ok := val.(int8)
		if !ok {
			return fmt.Errorf("invalid data provided")
		}
		header.Stat = v
		buf, err = SerializeHeader(header)
		if err != nil {
			return err
		}
	case HD_ADDR:

		v, ok := val.(DskAddr)
		if !ok {
			return fmt.Errorf("invalid data provided")
		}
		header.Addr = v
		buf, err = SerializeHeader(header)
		if err != nil {
			return err
		}

	case HD_SIZE:

		v, ok := val.(int64)
		if !ok {
			return fmt.Errorf("invalid data provided")
		}
		header.Size = v
		buf, err = SerializeHeader(header)
		if err != nil {
			return err
		}

	case HD_TYPE:

		v, ok := val.(int8)
		if !ok {
			return fmt.Errorf("invalid data provided")
		}
		header.Type = v
		buf, err = SerializeHeader(header)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("invalid header")
	}

	_, err = d.File.WriteAt(buf, int64(addr))
	if err != nil {
		return err
	}

	return nil
}

func (d *DiskManager) GetHeader(addr DskAddr) (*RecordHeader, error) {

	buf := make([]byte, HEADER_SIZE)
	n, err := d.File.ReadAt(buf, int64(addr))
	if err != nil || n == 0 {
		if err == nil {
			err = fmt.Errorf("header of size zero")
		}
		return nil, err
	}

	hdr, err := DeserializeHeader(buf)
	if err != nil {
		return nil, err
	}
	return hdr, nil
}

func (d *DiskManager) WrtDiskData(data *DiskData) error {

	d.mu.Lock()
	defer d.mu.Unlock()

	data.Header.Addr = d.EndOffset

	bufData, err := SerializeData(data.Data)
	if err != nil {
		return err
	}

	data.Header.Size = int64(len(bufData))
	bufHeader, err := SerializeHeader(&data.Header)
	if err != nil {
		return err
	}

	bufDiskData := append(bufHeader, bufData...)

	_, err = d.File.WriteAt(bufDiskData, int64(d.EndOffset))
	if err != nil {
		return err
	}
	d.EndOffset += DskAddr(len(bufDiskData))

	return nil
}

func (d *DiskManager) GetDiskData(addr int64) (*DiskData, error) {

	d.mu.Lock()
	defer d.mu.Unlock()

	bufHeader := make([]byte, HEADER_SIZE)
	_, err := d.File.ReadAt(bufHeader, addr)
	if err != nil {
		return nil, err
	}

	header, err := DeserializeHeader(bufHeader)
	if err != nil {
		return nil, err
	}

	bufData := make([]byte, header.Size)
	_, err = d.File.ReadAt(bufData, addr+HEADER_SIZE)
	if err != nil {
		return nil, err
	}

	data, err := DeserializeData(bufData, header.Type)
	if err != nil {
		return nil, err
	}

	return &DiskData{
		Data:   data,
		Header: *header,
	}, nil

}
