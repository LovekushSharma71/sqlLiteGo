package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"unsafe"
)

// const ID_OFFSET uint32 = 0
// const USERNAME_OFFSET uint32 = ID_OFFSET + ID_SIZE
// const EMAIL_OFFSET uint32 = USERNAME_OFFSET + USERNAME_SIZE
// const ROW_SIZE uint32 = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

const ID_SIZE = int(unsafe.Sizeof(Row{}.Id))
const USERNAME_SIZE = int(unsafe.Sizeof(Row{}.Username))
const EMAIL_SIZE = int(unsafe.Sizeof(Row{}.Email))
const ROW_SIZE = int(unsafe.Sizeof(Row{}))

const PAGE_SIZE = 4096
const TABLE_MAX_PAGES = 10
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES
const FILE_PATH = "data"

var BINARY_ORDER binary.ByteOrder = binary.LittleEndian

// table structure col : id-int|user-varchar(255)|email-varchar(255)
type Row struct {
	Id       int32
	Username [255]byte
	Email    [255]byte
}

type Pager struct {
	FData os.File
	FInfo os.FileInfo
	Pages [TABLE_MAX_PAGES][ROWS_PER_PAGE][]byte
}

type Table struct {
	NumRows uint32
	Pager   *Pager
}

func NewTable() *Table {

	file, err := os.OpenFile(FILE_PATH, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Errorf("Error opening a file:%+v\n", err)
		return nil
	}

	finfo, err := file.Stat()
	if err != nil {
		fmt.Errorf("Error getting a file info:%+v\n", err)
		return nil
	}

	if finfo.Size() > int64(TABLE_MAX_PAGES*TABLE_MAX_ROWS*ROW_SIZE) {
		fmt.Errorf("Table size limit exceeded\n")
		return nil
	}

	table := &Table{
		Pager: &Pager{
			FData: *file,
			FInfo: finfo,
			Pages: [TABLE_MAX_PAGES][ROWS_PER_PAGE][]byte{},
		},
	}
	return table
}

func (t *Table) SyncFile2Table() error {

	var fileOffset int64 = 0
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		for j := 0; j < ROWS_PER_PAGE; j++ {

			b := make([]byte, ROW_SIZE)
			n, err := t.Pager.FData.ReadAt(b, fileOffset)

			fmt.Println("Read", n, "bytes at offset", fileOffset, "Error:", err)

			if err != io.EOF && err != nil {
				fmt.Println("Error reading file:", err)
				return err
			}
			if n > 0 {
				t.Pager.Pages[i][j] = b[:n] // Store only the bytes read
				t.NumRows++
				fileOffset += int64(ROW_SIZE)
			} else {
				fmt.Println("Read 0 bytes, assuming EOF")
				return err
			}
			if err == io.EOF {
				fmt.Println("Reached EOF")
				return err
			}
		}
	}
	return nil
}

func (t *Table) SyncTable2File() {

	var offset int64 = 0
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		for j := 0; j < ROWS_PER_PAGE; j++ {

			if len(t.Pager.Pages[i][j]) > 0 {

				_, err := t.Pager.FData.WriteAt(t.Pager.Pages[i][j], offset)
				if err != nil {
					fmt.Errorf("Error while write:%v \n", err.Error())
					return
				}
				offset += int64(ROW_SIZE)
			} else {
				return
			}

		}
	}
}

func SerializeRow(src *Row) ([]byte, error) {

	buf := new(bytes.Buffer)
	err := binary.Write(buf, BINARY_ORDER, &src.Id)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, BINARY_ORDER, &src.Username)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, BINARY_ORDER, &src.Email)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeRow(src []byte) (*Row, error) {

	var rowData *Row = &Row{}
	reader := bytes.NewReader(src)
	err := binary.Read(reader, BINARY_ORDER, &rowData.Id)
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, BINARY_ORDER, &rowData.Username)
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, BINARY_ORDER, &rowData.Email)
	if err != nil {
		return nil, err
	}
	return rowData, nil
}

func (t *Table) GetRowFromTable(rowNum uint32) ([]byte, error) {

	pageNum := rowNum / uint32(ROWS_PER_PAGE)
	if pageNum > TABLE_MAX_PAGES {
		return nil, fmt.Errorf("Max page number exceeded: (Max pages allowed) %d < current page %d\n", TABLE_MAX_PAGES, pageNum)
	}
	page := t.Pager.Pages[pageNum]
	row := rowNum % uint32(ROWS_PER_PAGE)
	return page[row], nil
}
