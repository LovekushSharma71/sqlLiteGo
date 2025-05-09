package DiskManager

import (
	"encoding/binary"
	"os"
	"sync"
)

const (
	DB_FILE     = "Data/"
	HEADER_SIZE = 17
)

const (
	DT_STRING = iota
	DT_BYTES
	DT_INT64
	DT_INT32
	DT_INT16
	DT_INT8
)

var BINARY_ORDER = binary.BigEndian

type RecordHeader struct {
	Addr int64
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
	SrtOffset int64
	EndOffset int64
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
		EndOffset: size,
		mu:        sync.Mutex{},
	}, nil

}

func (d *DiskManager) Close() error {
	return d.File.Close()
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

	_, err = d.File.WriteAt(bufDiskData, d.EndOffset)
	if err != nil {
		return err
	}
	d.EndOffset += int64(len(bufDiskData))

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


