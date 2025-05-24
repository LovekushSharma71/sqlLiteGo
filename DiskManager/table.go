package diskmanager

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

func (t *DiskManager) Insert(key int32, val string) error {

	if len(val) > 32 {
		return fmt.Errorf("ListInsert error: val size length is greater than 32")
	}

	var buf [32]byte
	copy(buf[:], []byte(val))
	t.Cursor = t.SrtOff
	for {
		dsk, err := t.GetDiskData()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("ListInsert error: %w", err)
		}
		lp := dsk.RecData.(ListPage)
		if lp.Chld == -1 {
			break
		}
		t.Cursor = lp.Chld
	}

	dskData, err := t.GetDiskData()
	if errors.Is(err, io.EOF) {
		_, err := t.WrtDiskData(ListPage{
			Head: ListHead{
				Parent: -1,
			},
			Data: [MAX_KEYS]DataNode{{Key: key, Val: buf}},
			Chld: -1,
		})
		if err != nil {
			return fmt.Errorf("ListInsert error: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("ListInsert error: %s", err)
	} else {
		nodes := dskData.RecData.(ListPage).Data
		var ind int = -1
		for i := 0; i < MAX_KEYS; i++ {
			if IsNodeEmpty(nodes[i]) {
				ind = i
				break
			}
		}
		listPage := dskData.RecData.(ListPage)
		if ind != -1 {
			listPage.Data[ind] = DataNode{
				Key: key,
				Val: buf,
			}
		} else {

			newNodes := [MAX_KEYS]DataNode{}
			newNodes[0] = DataNode{
				Key: key,
				Val: buf,
			}
			dsk, err := t.WrtDiskData(ListPage{
				Head: ListHead{
					Parent: dskData.RecHead.RecAddr,
				},
				Data: newNodes,
				Chld: -1,
			})
			listPage.Chld = dsk.RecHead.RecAddr
			if err != nil {
				return fmt.Errorf("ListInsert error: %s", err)
			}
			fmt.Printf("List Node Inserted in disk:%+v", dsk)
		}
		err := t.EdtDiskData(listPage)
		if err != nil {
			return fmt.Errorf("ListInsert error: %s", err)
		}
	}
	return nil
}

func (t *DiskManager) Select(key int32) ([32]byte, error) {

	t.Cursor = t.SrtOff
	for {
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
		if lp.Chld == -1 {
			break
		}
		t.Cursor = lp.Chld
	}
	return [32]byte{}, nil
}

func (t *DiskManager) Update(key int32, val string) error {

	if len(val) > 32 {
		return fmt.Errorf("ListInsert error: val size length is greater than 32")
	}

	t.Cursor = t.SrtOff
	for {
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
		if lp.Chld == -1 {
			break
		}
		t.Cursor = lp.Chld

	}
	return fmt.Errorf("ListUpdate error: key not found")
}

func (t *DiskManager) Delete(key int32) error {

	t.Cursor = t.SrtOff
	for {
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
		if lp.Chld == -1 {
			break
		}
		t.Cursor = lp.Chld
	}
	return fmt.Errorf("ListDelete error: key not found")

}

func (t *DiskManager) SelectAll() error {

	t.Cursor = t.SrtOff
	for {
		fmt.Println(t.Cursor)
		dsk, err := t.GetDiskData()
		if err != nil {
			return fmt.Errorf("ListSelect error: %s", err.Error())
		}
		lp := dsk.RecData.(ListPage)
		for i := 0; i < MAX_KEYS; i++ {
			fmt.Printf("key: %d , Value: %s\n", lp.Data[i].Key, string(bytes.TrimRight(lp.Data[i].Val[:], "\x00")))
		}
		if lp.Chld == -1 {
			break
		}
		t.Cursor = lp.Chld

	}
	return nil
}
