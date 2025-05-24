package diskmanager

import (
	"encoding/binary"
	"os"
	"sync"
)

var (
	TREE_FILE            string = "Data/tree"
	LIST_FILE            string = "Data/list"
	TEST_FILE            string = "Data/test"
	HEADER_SIZE          int    = binary.Size(DskDataHdr{})
	BINARY_ORDER                = binary.BigEndian
	TBL_HEAD_SIZE        int    = binary.Size(TableHeader{})
	TREE_PAGE_SIZE       int    = binary.Size(TreePage{})
	LINEAR_PAGE_SIZE     int    = binary.Size(ListPage{})
	TREE_DISKDATA_SIZE   int    = HEADER_SIZE + TREE_PAGE_SIZE
	LINEAR_DISKDATA_SIZE int    = HEADER_SIZE + LINEAR_PAGE_SIZE
)

const (
	DB_FILE      string = "Data/"
	TREE_ORDER   int    = 3
	MAX_CHILDREN int    = TREE_ORDER
	MIN_CHILDREN int    = (TREE_ORDER + 1) / 2
	MAX_KEYS     int    = TREE_ORDER - 1
	MIN_KEYS     int    = (TREE_ORDER+1)/2 - 1
)

const (
	DT_LIST_PAGE = iota
	DT_TREE_PAGE
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

type DskDataHdr struct {
	Deleted bool
	RecAddr int32
	RecSize int32
	RecType int8
}

type DiskData struct {
	RecHead DskDataHdr
	RecData interface{}
}

type DiskManager struct {
	FilObj *os.File
	SrtOff int32
	Cursor int32
	EndOff int32
	IsTree bool
	MuLock sync.Mutex // just in case
}

type TableHeader struct {
	RootAddr int32
	IsLinear bool
}

type TreePage struct {
	Head TreeHead
	Data [MAX_KEYS]DataNode
	Chld [MAX_CHILDREN]int32
}

type TreeHead struct {
	IsLeaf bool
	IsRoot bool
	Parent int32
}

type ListPage struct {
	Head ListHead
	Data [MAX_KEYS]DataNode
	Chld int32
}

type ListHead struct {
	Parent int32
}

type DataNode struct {
	Key int32
	Val [32]byte
}
