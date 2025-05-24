package diskmanager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

func CreateDatabase(dbname string, dbtype string) error {

	dbFile := DB_FOLDER + "/" + dbname
	found, err := DBExists(dbFile)
	if found {
		return fmt.Errorf("CreateDatabase error: database already exists")
	}
	if err != nil {
		return fmt.Errorf("CreateDatabase error: %w", err)
	}
	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("CreateDatabase error, create file error: %w", err)
	}

	buf := new(bytes.Buffer)

	tblHead := TableHeader{
		RootAddr: int32(TBL_HEAD_SIZE),
	}
	switch dbtype {
	case "tree":
		tblHead.IsLinear = false
	case "list":
		tblHead.IsLinear = true
	default:
		return fmt.Errorf("CreateDatabase error: invalid table type")
	}

	err = binary.Write(buf, BINARY_ORDER, tblHead)
	if err != nil {
		return fmt.Errorf("CreateDatabase error, header to bytes failed: %w", err)
	}

	file.WriteAt(buf.Bytes(), 0)

	return nil
}

func InitDatabase(dbname string) (*DiskManager, error) {

	dbFile := DB_FOLDER + "/" + dbname
	found, err := DBExists(dbFile)
	if !found {
		return nil, fmt.Errorf("InitDatabase error: database does not exists")
	}
	if err != nil {
		return nil, fmt.Errorf("InitDatabase error: %w", err)
	}

	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("InitDatabase error, create file error: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("InitDiskManager error: %w", err)
	}
	size := info.Size()

	buf := make([]byte, TBL_HEAD_SIZE)

	_, err = file.ReadAt(buf, 0)
	if err != nil {
		return nil, fmt.Errorf("InitDatabase error, read file error: %w", err)
	}

	reader := bytes.NewReader(buf)
	th := &TableHeader{}
	err = binary.Read(reader, BINARY_ORDER, th)
	if err != nil {
		return nil, fmt.Errorf("InitDatabase error, header decode error: %w", err)
	}

	dskMan := &DiskManager{
		FilObj: file,
		SrtOff: th.RootAddr,
		Cursor: th.RootAddr,
		EndOff: int32(size),
		IsTree: !th.IsLinear,
		MuLock: sync.Mutex{},
	}

	return dskMan, nil
}
