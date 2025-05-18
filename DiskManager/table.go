package diskmanager

import (
	"fmt"
)

func (t *DiskManager) Insert(key int32, val string) error {

	if len(val) > 32 {
		return fmt.Errorf("ListInsert error: val size length is greater than 32")
	}

	var buf [32]byte
	copy(buf[:], []byte(val))
	t.Cursor = t.EndOff - int32(LINEAR_PAGE_SIZE)
	dskData, err := t.GetDiskData()
	if err != nil {
		return fmt.Errorf("ListInsert error: %s", err)
	}
	nodes := dskData.RecData.(ListPage).Data
	var ind int = -1
	for i := 0; i < MAX_KEYS; i++ {
		if IsNodeEmpty(nodes[i]) {
			ind = i
		}
	}
	if ind != -1 {
		nodes[ind] = DataNode{
			Key: key,
			Val: buf,
		}
		dskData.RecData = nodes
		err := t.EdtDiskData(dskData.RecData)
		if err != nil {
			return fmt.Errorf("ListInsert error: %s", err)
		}
	} else {
		nodes[0] = DataNode{
			Key: key,
			Val: buf,
		}
		t.Cursor = t.EndOff
		dsk, err := t.WrtDiskData(ListPage{
			Head: ListHead{
				Parent: dskData.RecHead.RecAddr,
			},
			Data: nodes,
			Chld: t.EndOff,
		})
		if err != nil {
			return fmt.Errorf("ListInsert error: %s", err)
		}
		fmt.Printf("List Node Inserted in disk:%+v", dsk)
	}

	return nil
}

func (t *DiskManager) Select(key int32) ([32]byte, error) {

	for t.Cursor = t.SrtOff; t.Cursor < t.EndOff; {
		dsk, err := t.GetDiskData()
		if err != nil {
			return [32]byte{}, fmt.Errorf("ListSelect error: %s", err.Error())
		}
		lp := dsk.RecData.(ListPage)
		for i := 0; i < MAX_KEYS; i++ {
			if lp.Data[i].Key == key {
				return lp.Data[i].Val, nil
			}
		}
		t.Cursor = lp.Chld
	}
	return [32]byte{}, nil
}

func (t *DiskManager) Update(key int32, val string) error {

	for t.Cursor = t.SrtOff; t.Cursor < t.EndOff; {
		dsk, err := t.GetDiskData()
		if err != nil {
			return fmt.Errorf("ListUpdate error: %s", err.Error())
		}
		lp := dsk.RecData.(ListPage)
		isUpdated := false
		for i := 0; i < MAX_KEYS; i++ {
			if lp.Data[i].Key == key {
				buf := [32]byte{}
				copy(buf[:], []byte(val))
				lp.Data[i].Val = buf
				isUpdated = true
				break
			}
		}
		if isUpdated {
			err := t.EdtDiskData(lp)
			if err != nil {
				return fmt.Errorf("ListUpdate error: %s", err.Error())
			}
			return nil
		}
		t.Cursor = lp.Chld

	}
	return fmt.Errorf("ListUpdate error: key not found")
}

func (t *DiskManager) Delete(key int32) error {

	for t.Cursor = t.SrtOff; t.Cursor < t.EndOff; {
		dsk, err := t.GetDiskData()
		if err != nil {
			return fmt.Errorf("ListDelete error: %s", err.Error())
		}
		lp := dsk.RecData.(ListPage)
		isDeleted := false
		for i := 0; i < MAX_KEYS; i++ {
			if lp.Data[i].Key == key {
				if i == 0 {
					t.Cursor = lp.Head.Parent
					dsk, err = t.GetDiskData()
					if err != nil {
						return fmt.Errorf("ListDelete error: %s", err.Error())
					}
					tp := dsk.RecData.(ListPage)
					tp.Chld = lp.Chld
					dsk.RecData = tp

				} else {
					lp.Data[i] = DataNode{}
				}
				isDeleted = true
				break
			}
		}
		if isDeleted {
			err := t.EdtDiskData(lp)
			if err != nil {
				return fmt.Errorf("ListDelete error: %s", err.Error())
			}
			return nil
		}
		t.Cursor = lp.Chld
	}
	return fmt.Errorf("ListDelete error: key not found")

}

func (t *DiskManager) SelectAll(key int32) error {

	for t.Cursor = t.SrtOff; t.Cursor < t.EndOff; {
		dsk, err := t.GetDiskData()
		if err != nil {
			return fmt.Errorf("ListSelect error: %s", err.Error())
		}
		lp := dsk.RecData.(ListPage)
		for i := 0; i < MAX_KEYS; i++ {
			fmt.Printf("key: %d , Value: %s", lp.Data[i].Key, lp.Data[i].Val)
		}
		t.Cursor = lp.Chld

	}
	return nil
}
