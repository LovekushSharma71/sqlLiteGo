package diskmanager

import (
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

// rewrite insert
func (t tree) Insert(key int32, val string) error {
	// dsk, err := t.table.GetDiskData()
	// if errors.Is(err, io.EOF) && t.table.Cursor == t.table.EndOff {
	// 	t.table.WrtDiskData(TreePage{
	// 		Head: TreeHead{
	// 			IsLeaf: true,
	// 			IsRoot: true,
	// 			Parent: -1,
	// 		},
	// 		Data: [MAX_KEYS]DataNode{{Key: key, Val: String2ByteArr(val)}},
	// 		Chld: [MAX_CHILDREN]int32{},
	// 	})
	// 	return nil
	// }
	// if err != nil {
	// 	return fmt.Errorf("TreeInsert Error:%w", err)
	// }
	// tp := dsk.RecData.(TreePage)
	// if tp.Head.IsLeaf {
	// 	// optimise this sometime in future
	// 	var NodeBuf []DataNode = append(tp.Data[:], DataNode{
	// 		Key: key,
	// 		Val: String2ByteArr(val),
	// 	})
	// 	sort.Slice(NodeBuf, func(i, j int) bool {
	// 		return NodeBuf[i].Key < NodeBuf[j].Key
	// 	})
	// 	if len(NodeBuf) <= MAX_KEYS {
	// 		copy(tp.Data[:], NodeBuf)
	// 		// tp.Data = [MAX_KEYS]DataNode(NodeBuf)
	// 		t.table.EdtDiskData(tp)
	// 		return nil
	// 	}
	// 	copy(tp.Data[:], NodeBuf[:len(NodeBuf)/2])
	// 	// tp.Data = [MAX_KEYS]DataNode(NodeBuf[:len(NodeBuf)/2])
	// 	tp.Head.IsRoot = false
	// 	var tmp [MAX_KEYS]DataNode = [MAX_KEYS]DataNode{}
	// 	copy(tmp[:], NodeBuf[len(NodeBuf)/2+1:])

	// 	// tp.Data = [MAX_KEYS]DataNode(NodeBuf[:len(NodeBuf)/2])
	// 	tp1 := TreePage{
	// 		Head: tp.Head,
	// 		Data: tmp,
	// 		Chld: tp.Chld,
	// 	}
	// 	err = t.table.EdtDiskData(tp)
	// 	if err != nil {
	// 		return fmt.Errorf("TreeInsert Error:%w", err)
	// 	}
	// 	nd, err := t.table.WrtDiskData(tp1)
	// 	if err != nil {
	// 		return fmt.Errorf("TreeInsert Error:%w", err)
	// 	}
	// 	var in InsertKeyError = InsertKeyError{
	// 		PromotedNode: NodeBuf[len(NodeBuf)/2],
	// 		NewChildNode: nd.RecHead.RecAddr,
	// 		Err:          nil,
	// 	}
	// 	return &in
	// }
	// ind := -1
	// for i := 0; i < MAX_KEYS; i++ {
	// 	if tp.Data[i].Key > key {
	// 		ind = i
	// 		break
	// 	}
	// }
	// if ind == -1 {
	// 	t.table.Cursor = tp.Chld[MAX_CHILDREN-1]
	// } else {
	// 	t.table.Cursor = tp.Chld[ind]
	// }
	// err = t.Insert(key, val)
	// if errors.Is(err, &InsertKeyError{}) {
	// 	dn, cn := err.(*InsertKeyError).PromotedNode, err.(*InsertKeyError).NewChildNode
	// 	var NodeBuf []DataNode = append(append(tp.Data[:ind], dn), tp.Data[ind:]...)
	// 	var ChldBuf []int32 = append(append(tp.Chld[:ind+1], cn), tp.Chld[ind+1:]...)
	// 	if len(NodeBuf) <= MAX_KEYS {
	// 		tp.Data = [MAX_KEYS]DataNode(NodeBuf)
	// 		tp.Chld = [MAX_CHILDREN]int32(ChldBuf)
	// 		return nil
	// 	}
	// 	if tp.Head.IsRoot {

	// 		copy(tp.Data[:], NodeBuf[:len(NodeBuf)/2])
	// 		// tp.Data = [MAX_KEYS]DataNode(NodeBuf[:len(NodeBuf)/2])
	// 		tp.Head.IsRoot = false
	// 		var tmp [MAX_KEYS]DataNode = [MAX_KEYS]DataNode{}
	// 		copy(tmp[:], NodeBuf[len(NodeBuf)/2+1:])
	// 		tp1 := TreePage{
	// 			Head: tp.Head,
	// 			Data: tmp,
	// 			Chld: tp.Chld,
	// 		}

	// 		err = t.table.EdtDiskData(tp)
	// 		if err != nil {
	// 			return fmt.Errorf("TreeInsert Error:%w", err)
	// 		}
	// 		nd, err := t.table.WrtDiskData(tp1)
	// 		if err != nil {
	// 			return fmt.Errorf("TreeInsert Error:%w", err)
	// 		}
	// 		t.table.WrtDiskData(TreePage{
	// 			Head: TreeHead{
	// 				IsLeaf: false,
	// 				IsRoot: true,
	// 				Parent: -1,
	// 			},
	// 			Data: [MAX_KEYS]DataNode{NodeBuf[len(NodeBuf)/2]},
	// 			Chld: [MAX_CHILDREN]int32{dsk.RecHead.RecAddr, nd.RecHead.RecAddr},
	// 		})
	// 		return nil
	// 	}
	// 	copy(tp.Data[:], NodeBuf[:len(NodeBuf)/2])
	// 	// tp.Data = [MAX_KEYS]DataNode(NodeBuf[:len(NodeBuf)/2])
	// 	tp.Head.IsRoot = false
	// 	var tmp [MAX_KEYS]DataNode = [MAX_KEYS]DataNode{}
	// 	copy(tmp[:], NodeBuf[len(NodeBuf)/2+1:])
	// 	tp1 := TreePage{
	// 		Head: tp.Head,
	// 		Data: tmp,
	// 		Chld: tp.Chld,
	// 	}
	// 	err = t.table.EdtDiskData(tp)
	// 	if err != nil {
	// 		return fmt.Errorf("TreeInsert Error:%w", err)
	// 	}
	// 	nd, err := t.table.WrtDiskData(tp1)
	// 	if err != nil {
	// 		return fmt.Errorf("TreeInsert Error:%w", err)
	// 	}
	// 	var in InsertKeyError = InsertKeyError{
	// 		PromotedNode: NodeBuf[len(NodeBuf)/2],
	// 		NewChildNode: nd.RecHead.RecAddr,
	// 		Err:          nil,
	// 	}
	// 	return &in
	// }
	// if err != nil {
	// 	return fmt.Errorf("TreeInsert Error:%w", err)
	// }

	return nil
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
