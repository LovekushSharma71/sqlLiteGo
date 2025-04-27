package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

var BINARY_ORDER binary.ByteOrder = binary.LittleEndian

// table structure col : id-int|user-varchar(255)|email-varchar(255)
type Row struct {
	Id       int32
	Username [255]byte
	Email    [255]byte
}

type Table struct {
	NumRows uint32
	Pages   [TABLE_MAX_PAGES][ROWS_PER_PAGE][]byte
}

func NewTable() *Table {
	table := &Table{
		Pages: [TABLE_MAX_PAGES][ROWS_PER_PAGE][]byte{},
	}
	return table
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
		return nil, fmt.Errorf("Max page number exceeded: (Max pages allowed) %d < current page %d", TABLE_MAX_PAGES, pageNum)
	}
	page := t.Pages[pageNum]
	row := rowNum % uint32(ROWS_PER_PAGE)
	return page[row], nil
}
