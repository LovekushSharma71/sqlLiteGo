package diskmanager

import (
	"errors"
	"fmt"
)

type tree struct {
	table *DiskManager
}

// helps in split logic maintaining function signature
type InsertKeyError struct {
	PromotedNode DataNode
	NewChildNode int32
	Err          error
}

func (e *InsertKeyError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("promoted key %d, new child ref %v: %s", e.PromotedNode.Key, e.NewChildNode, e.Err.Error())
	}
	return fmt.Sprintf("promoted key %d, new child ref %v", e.PromotedNode.Key, e.NewChildNode)
}

// Unwrap allows this error to be unwrapped to reveal the underlying error
func (e *InsertKeyError) Unwrap() error {
	return e.Err
}

func (t tree) ResetCursor() error {

	hdr, err := t.table.GetDBHeader()
	if err != nil {
		return fmt.Errorf("Tree ResetCursor Error:%w", err)
	}

	t.table.Cursor = hdr.RootAddr
	t.table.SrtOff = hdr.RootAddr
	return nil
}

// rewrite insert
func (t tree) Insert(key int32, val string) error {

	if t.table.SrtOff == t.table.EndOff {
		// if table is empty, create a new root node
		t.table.WrtDiskData(TreePage{
			Head: TreeHead{
				IsLeaf: true,
				IsRoot: true,
				Parent: -1,
			},
			Data: [MAX_KEYS]DataNode{{Key: key, Val: String2ByteArr(val)}},
			Chld: [MAX_CHILDREN]int32{},
		})
		t.table.WrtDBHeader(TableHeader{
			RootAddr: t.table.SrtOff,
			IsLinear: false,
		})
		return nil

	}
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("TreeInsert Error:%w", err)
	}
	tp := dsk.RecData.(TreePage)
	if tp.Head.IsLeaf {
		fmt.Println("inside leaf")
		// if the current page is a leaf, we can insert directly
		var NodeBuf []DataNode
		isInserted := false
		for _, v := range tp.Data {
			if IsNodeEmpty(v) {
				break
			}
			if v.Key > key {
				isInserted = true
				NodeBuf = append(NodeBuf, DataNode{
					Key: key,
					Val: String2ByteArr(val),
				})

			}
			NodeBuf = append(NodeBuf, v)
		}
		if !isInserted {
			NodeBuf = append(NodeBuf, DataNode{
				Key: key,
				Val: String2ByteArr(val),
			})
		}
		tp.Data = [MAX_KEYS]DataNode{}
		if len(NodeBuf) <= MAX_KEYS {

			copy(tp.Data[:], NodeBuf)
			err = t.table.EdtDiskData(tp)
			if err != nil {
				return fmt.Errorf("TreeInsert Error:%w", err)
			}
			return nil

		}
		// if the current page is full, we need to split it
		var tmpPge1, tmpPge2 TreePage

		copy(tmpPge1.Data[:], NodeBuf[:len(NodeBuf)/2])
		tmpPge1.Head = tp.Head
		tmpPge1.Head.IsRoot = false
		// page has no children, so we can just copy the existing leaf page children
		tmpPge1.Chld = tp.Chld

		copy(tmpPge2.Data[:], NodeBuf[len(NodeBuf)/2+1:])
		tmpPge2.Head = tp.Head
		tmpPge2.Head.IsRoot = false
		// page has no children, so we can just copy the existing leaf page children
		tmpPge2.Chld = tp.Chld

		tmpCursor := t.table.Cursor
		t.table.Cursor = dsk.RecHead.RecAddr
		err = t.table.EdtDiskData(tmpPge1)
		if err != nil {
			return fmt.Errorf("TreeInsert Error:%w", err)
		}
		nd, err := t.table.WrtDiskData(tmpPge2)
		if err != nil {
			return fmt.Errorf("TreeInsert Error:%w", err)
		}
		t.table.Cursor = tmpCursor
		if tp.Head.IsRoot {
			root, err := t.table.WrtDiskData(TreePage{
				Head: TreeHead{
					IsLeaf: false,
					IsRoot: true,
					Parent: -1,
				},
				Data: [MAX_KEYS]DataNode{{Key: NodeBuf[len(NodeBuf)/2].Key, Val: NodeBuf[len(NodeBuf)/2].Val}},
				Chld: [MAX_CHILDREN]int32{dsk.RecHead.RecAddr, nd.RecHead.RecAddr},
			})
			if err != nil {
				return fmt.Errorf("TreeInsert Error:%w", err)
			}
			t.table.WrtDBHeader(TableHeader{
				RootAddr: root.RecHead.RecAddr,
				IsLinear: false,
			})
			t.table.SrtOff = root.RecHead.RecAddr
			// update parent children later
			return nil
		}
		return &InsertKeyError{
			PromotedNode: NodeBuf[len(NodeBuf)/2],
			NewChildNode: nd.RecHead.RecAddr,
			Err:          nil,
		}
	}
	// if the current page is not a leaf, we need to find the correct child node to insert into
	// find the child node to insert into

	var promtNode DataNode
	var chldAddr int32 = -1
	tmpCursor := t.table.Cursor
	found := false
	numValidKeys := 0
	for i, v := range tp.Data {
		if v.Key > key {
			found = true
			t.table.Cursor = tp.Chld[i]
			break
		}
		numValidKeys++
	}
	if !found {
		t.table.Cursor = tp.Chld[numValidKeys]
	}
	err = t.Insert(key, val)
	t.table.Cursor = tmpCursor
	fmt.Println("insert result", err)
	if errors.As(err, &InsertKeyError{}) {
		promtNode, chldAddr = err.(*InsertKeyError).PromotedNode, err.(*InsertKeyError).NewChildNode
		key = promtNode.Key
	} else if err != nil {
		return fmt.Errorf("TreeInsert Error:%w", err)
	} else {
		return nil
	}

	insertIdx := 0
	numCurrentKeys := 0
	for _, kv := range tp.Data {
		if IsNodeEmpty(kv) {
			break
		}
		numCurrentKeys++
		if key < kv.Key {
			break
		}
		insertIdx++
	}

	var NodeBuf []DataNode
	var chldBuf []int32

	copy(NodeBuf[:insertIdx], tp.Data[:insertIdx])
	copy(chldBuf[:insertIdx+1], tp.Chld[:insertIdx+1])

	NodeBuf = append(NodeBuf, DataNode{Key: key, Val: promtNode.Val})
	chldBuf = append(chldBuf, chldAddr)

	copy(NodeBuf[insertIdx+1:], tp.Data[insertIdx:numCurrentKeys])
	copy(chldBuf[insertIdx+2:], tp.Chld[insertIdx+1:numCurrentKeys+1])

	fmt.Printf("NodeBuf:%+v\n", NodeBuf)
	fmt.Printf("chldBuf:%+v\n", chldBuf)
	if len(NodeBuf) <= MAX_KEYS {
		tp.Data = [MAX_KEYS]DataNode{}
		tp.Chld = [MAX_CHILDREN]int32{}
		copy(tp.Data[:], NodeBuf)
		copy(tp.Chld[:], chldBuf)
		err = t.table.EdtDiskData(tp)
		if err != nil {
			return fmt.Errorf("TreeInsert Error:%w", err)
		}
		return nil
	}
	// if the current page is full, we need to split it
	var tmpPge1, tmpPge2 TreePage

	copy(tmpPge1.Data[:], NodeBuf[:len(NodeBuf)/2])
	tmpPge1.Head = tp.Head
	tmpPge1.Head.IsRoot = false
	copy(tmpPge1.Chld[:], chldBuf[:len(chldBuf)/2])

	copy(tmpPge2.Data[:], NodeBuf[len(NodeBuf)/2+1:])
	tmpPge2.Head = tp.Head
	tmpPge2.Head.IsRoot = false
	copy(tmpPge2.Chld[:], chldBuf[len(chldBuf)/2+1:])

	tmpCursor = t.table.Cursor
	t.table.Cursor = dsk.RecHead.RecAddr
	err = t.table.EdtDiskData(tmpPge1)
	if err != nil {
		return fmt.Errorf("TreeInsert Error:%w", err)
	}
	nd, err := t.table.WrtDiskData(tmpPge2)
	if err != nil {
		return fmt.Errorf("TreeInsert Error:%w", err)
	}
	t.table.Cursor = tmpCursor
	if tp.Head.IsRoot {
		// if the current page is a root, we need to create a new root node
		root, err := t.table.WrtDiskData(TreePage{
			Head: TreeHead{
				IsLeaf: true,
				IsRoot: true,
				Parent: -1,
			},
			Data: [MAX_KEYS]DataNode{{Key: NodeBuf[len(NodeBuf)/2].Key, Val: NodeBuf[len(NodeBuf)/2].Val}},
			Chld: [MAX_CHILDREN]int32{dsk.RecHead.RecAddr, nd.RecHead.RecAddr},
		})
		if err != nil {
			return fmt.Errorf("TreeInsert Error:%w", err)
		}
		t.table.WrtDBHeader(TableHeader{
			RootAddr: root.RecHead.RecAddr,
			IsLinear: false,
		})
		t.table.SrtOff = root.RecHead.RecAddr
		return nil

	}
	return &InsertKeyError{
		PromotedNode: NodeBuf[len(NodeBuf)/2],
		NewChildNode: nd.RecHead.RecAddr,
		Err:          nil,
	}
}
func (t tree) Select(key int32) (string, error) {

	return "", nil
}

func (t tree) Delete(key int32) error {
	return nil
}
func (t tree) Update(key int32, val string) error {
	return nil
}
func (t tree) SelectAll() error {
	return nil
}
