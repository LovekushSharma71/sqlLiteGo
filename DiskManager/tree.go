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
		return fmt.Errorf("tree: ResetCursor error:%w", err)
	}
	t.table.Cursor = hdr.RootAddr
	t.table.SrtOff = hdr.RootAddr
	return nil
}

func (t tree) updatePageParent(pageAddr int32, parentAddr int32, isRoot bool) error {

	if pageAddr == 0 || pageAddr == -1 {
		return fmt.Errorf("tree: updatePageParent: invalid pageAddr %d", pageAddr)
	}
	savedCursor := t.table.Cursor
	t.table.Cursor = pageAddr
	dskData, err := t.table.GetDiskData()
	if err != nil {
		t.table.Cursor = savedCursor
		return fmt.Errorf("tree: updatePageParent (get page %d): %w", pageAddr, err)
	}
	var pageToUpdate TreePage // Assuming all pages in the tree are TreePage
	if dskData.RecHead.RecType != DT_TREE_PAGE {
		t.table.Cursor = savedCursor
		return fmt.Errorf("tree: updatePageParent: page %d is not a TreePage, type %T", pageAddr, dskData.RecData)
	}
	pageToUpdate = dskData.RecData.(TreePage)
	pageToUpdate.Head.Parent = parentAddr
	pageToUpdate.Head.IsRoot = isRoot // Update IsRoot status as well
	err = t.table.EdtDiskData(pageToUpdate)
	t.table.Cursor = savedCursor
	if err != nil {
		return fmt.Errorf("tree: updatePageParent (edit page %d): %w", pageAddr, err)
	}
	return nil
}

func (t tree) Insert(key int32, val string) error {

	// if table is empty
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

	// if table is not empty, we need to insert into the tree
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("tree: Insert Error:%w", err)
	}
	currentPage := dsk.RecData.(TreePage)
	currentPageAddr := dsk.RecHead.RecAddr

	// if the current page is a leaf, we can insert directly
	if currentPage.Head.IsLeaf {

		var insertIdx, numCurrentKeys = 0, 0
		for _, v := range currentPage.Data {
			if IsNodeEmpty(v) {
				break
			}
			if v.Key < key {
				insertIdx++
			}
			numCurrentKeys++
		}

		var NodeBuf []DataNode = make([]DataNode, numCurrentKeys+1)

		copy(NodeBuf, currentPage.Data[:insertIdx])
		NodeBuf[insertIdx] = DataNode{
			Key: key,
			Val: String2ByteArr(val),
		}
		copy(NodeBuf[insertIdx+1:], currentPage.Data[insertIdx:numCurrentKeys])

		// if the current page is not full, we can just copy the existing leaf page children
		currentPage.Data = [MAX_KEYS]DataNode{}
		if len(NodeBuf) <= MAX_KEYS {

			copy(currentPage.Data[:], NodeBuf)
			err = t.table.EdtDiskData(currentPage)
			if err != nil {
				return fmt.Errorf("tree: Insert Error:%w", err)
			}
			return nil

		}

		// if the current page is full, we need to split it
		var leftPge, rightPge TreePage

		copy(leftPge.Data[:], NodeBuf[:len(NodeBuf)/2])
		leftPge.Head = currentPage.Head
		leftPge.Head.IsRoot = false
		// page has no children, so we can just copy the existing leaf page children
		leftPge.Chld = currentPage.Chld

		copy(rightPge.Data[:], NodeBuf[len(NodeBuf)/2+1:])
		rightPge.Head = currentPage.Head
		rightPge.Head.IsRoot = false
		// page has no children, so we can just copy the existing leaf page children
		rightPge.Chld = currentPage.Chld

		err = t.table.EdtDiskData(leftPge)
		if err != nil {
			return fmt.Errorf("tree: Insert Error:%w", err)
		}
		nd, err := t.table.WrtDiskData(rightPge)
		if err != nil {
			return fmt.Errorf("tree: Insert Error:%w", err)
		}
		rightPgeAddr := nd.RecHead.RecAddr // Address of the right page, to update parent later

		if currentPage.Head.IsRoot {
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
				return fmt.Errorf("tree: Insert Error:%w", err)
			}
			t.table.WrtDBHeader(TableHeader{
				RootAddr: root.RecHead.RecAddr,
				IsLinear: false,
			})
			t.table.SrtOff = root.RecHead.RecAddr
			t.table.Cursor = root.RecHead.RecAddr // Set cursor to the new root
			// Update parent pointers of the two new children
			if err := t.updatePageParent(currentPageAddr, root.RecHead.RecAddr, false); err != nil { // Left child
				return fmt.Errorf("TreeInsert (leaf root split: update parent of left child %d): %w", currentPageAddr, err)
			}
			if err := t.updatePageParent(rightPgeAddr, root.RecHead.RecAddr, false); err != nil { // Right child
				return fmt.Errorf("TreeInsert (leaf root split: update parent of right child %d): %w", rightPgeAddr, err)
			}
			return nil
		}
		return &InsertKeyError{
			PromotedNode: NodeBuf[len(NodeBuf)/2],
			NewChildNode: rightPgeAddr,
			Err:          nil,
		}
	}
	// internal node case, we need to find the child node to insert into
	savedCursor := t.table.Cursor
	foundChild := false
	numValidKeys := 0
	for i, v := range currentPage.Data {
		if IsNodeEmpty(v) {
			break
		}
		if v.Key > key {
			foundChild = true
			t.table.Cursor = currentPage.Chld[i]
			break
		}
		numValidKeys++
	}
	if !foundChild {
		t.table.Cursor = currentPage.Chld[numValidKeys]
	}

	// Recursive call to Insert
	err = t.Insert(key, val)
	t.table.Cursor = savedCursor

	var promotedNodeFromChild DataNode
	var newChildAddrFromPromotion int32
	if errors.As(err, new(*InsertKeyError)) {
		promotedNodeFromChild, newChildAddrFromPromotion = err.(*InsertKeyError).PromotedNode, err.(*InsertKeyError).NewChildNode
		key = promotedNodeFromChild.Key
	} else if err != nil {
		return fmt.Errorf("tree: Insert Error:%w", err)
	} else {
		return nil
	}

	// Insert promoted key and new child pointer into THIS internal node (tp)
	insertIdx := 0
	numCurrentKeys := 0
	for _, v := range currentPage.Data {
		if IsNodeEmpty(v) {
			break
		}
		numCurrentKeys++
		if key < v.Key {
			break
		}
		insertIdx++
	}
	var NodeBuf []DataNode = make([]DataNode, numCurrentKeys+1)
	var chldBuf []int32 = make([]int32, numCurrentKeys+2)

	copy(NodeBuf[:insertIdx], currentPage.Data[:insertIdx])
	copy(chldBuf[:insertIdx+1], currentPage.Chld[:insertIdx+1])

	NodeBuf[insertIdx] = DataNode{Key: key, Val: promotedNodeFromChild.Val}
	chldBuf[insertIdx+1] = newChildAddrFromPromotion

	copy(NodeBuf[insertIdx+1:], currentPage.Data[insertIdx:numCurrentKeys])
	copy(chldBuf[insertIdx+2:], currentPage.Chld[insertIdx+1:numCurrentKeys+1])

	if len(NodeBuf) <= MAX_KEYS {

		currentPage.Data = [MAX_KEYS]DataNode{}
		currentPage.Chld = [MAX_CHILDREN]int32{}
		copy(currentPage.Data[:], NodeBuf)
		copy(currentPage.Chld[:], chldBuf)
		if newChildAddrFromPromotion != 0 && newChildAddrFromPromotion != -1 {
			if err := t.updatePageParent(newChildAddrFromPromotion, currentPageAddr, false); err != nil {
				return fmt.Errorf("TreeInsert (internal no-split: update parent of new child %d): %w", newChildAddrFromPromotion, err)
			}
		}
		err = t.table.EdtDiskData(currentPage)
		if err != nil {
			return fmt.Errorf("tree: Insert Error:%w", err)
		}
		return nil
	}

	// internal node is full, we need to split it
	medianIdx := len(NodeBuf) / 2
	promotedNodeFromInternal := NodeBuf[medianIdx]

	leftIntenalPage := TreePage{Head: currentPage.Head}
	leftIntenalPage.Head.IsRoot = false // No longer root after split
	copy(leftIntenalPage.Data[:], NodeBuf[:medianIdx])
	copy(leftIntenalPage.Chld[:], chldBuf[:medianIdx+1]) // Children for left page

	rightInternalPage := TreePage{Head: currentPage.Head}
	rightInternalPage.Head.IsRoot = false
	copy(rightInternalPage.Data[:], NodeBuf[medianIdx+1:])
	copy(rightInternalPage.Chld[:], chldBuf[medianIdx+1:]) // Children for right page

	err = t.table.EdtDiskData(leftIntenalPage)
	if err != nil {
		return fmt.Errorf("tree: Insert Error:%w", err)
	}
	newRightInternalPage, err := t.table.WrtDiskData(rightInternalPage)
	if err != nil {
		return fmt.Errorf("tree: Insert Error:%w", err)
	}
	newRightInternalPageAddr := newRightInternalPage.RecHead.RecAddr // Address of the right page, to update parent later

	// Update parent pointers of children adopted by the new rightIntenalPage
	for _, childAddrMoved := range rightInternalPage.Chld { // Iterate only over children actually in rightInternalPage
		if childAddrMoved == 0 || childAddrMoved == -1 {
			continue
		}
		// Check if this child was indeed part of the moved set, to avoid re-parenting already correct ones
		// This check is implicitly handled if rightInternalPage.Chld only contains those that moved.
		if err := t.updatePageParent(childAddrMoved, newRightInternalPageAddr, false); err != nil {
			return fmt.Errorf("TreeInsert (internal split: update parent of child %d to new right page %d): %w", childAddrMoved, newRightInternalPageAddr, err)
		}
	}

	// If the split internal node was the ROOT
	if currentPage.Head.IsRoot {

		rewRoot, err := t.table.WrtDiskData(TreePage{
			Head: TreeHead{
				IsLeaf: false, // Root of internal nodes is not a leaf
				IsRoot: true,
				Parent: -1,
			},
			Data: [MAX_KEYS]DataNode{promotedNodeFromInternal},
			Chld: [MAX_CHILDREN]int32{currentPageAddr, newRightInternalPageAddr},
		})
		if err != nil {
			return fmt.Errorf("tree: Insert Error:%w", err)
		}
		err = t.table.WrtDBHeader(TableHeader{
			RootAddr: rewRoot.RecHead.RecAddr,
			IsLinear: false,
		})
		if err != nil {
			return fmt.Errorf("tree: Insert Error:%w", err)
		}
		t.table.SrtOff = rewRoot.RecHead.RecAddr
		t.table.Cursor = rewRoot.RecHead.RecAddr // Set cursor to the new root

		// Update parent pointers of the two new children (split internal nodes)
		if err := t.updatePageParent(currentPageAddr, rewRoot.RecHead.RecAddr, false); err != nil { // Left child
			return fmt.Errorf("TreeInsert (internal root split: update parent of left child %d): %w", currentPageAddr, err)
		}
		if err := t.updatePageParent(newRightInternalPageAddr, rewRoot.RecHead.RecAddr, false); err != nil { // Right child
			return fmt.Errorf("TreeInsert (internal root split: update parent of right child %d): %w", newRightInternalPageAddr, err)
		}
	}

	return &InsertKeyError{
		PromotedNode: promotedNodeFromInternal,
		NewChildNode: newRightInternalPageAddr,
		Err:          nil,
	}
}

func (t tree) Select(key int32) (string, error) {

	// if table is empty
	if t.table.SrtOff == t.table.EndOff {
		return "", fmt.Errorf("tree: SelectAll Error: table is empty")
	}
	// if table is not empty, we need to select all from the tree
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return "", fmt.Errorf("tree: SelectAll Error:%w", err)
	}
	currentPage := dsk.RecData.(TreePage)
	if currentPage.Head.IsLeaf {
		for _, v := range currentPage.Data {
			if IsNodeEmpty(v) {
				break
			}
			if v.Key == key {
				return ByteArr2String(v.Val), nil
			}
		}
		return "", fmt.Errorf("tree: Select Error: key %d not found", key)
	}
	if currentPage.Data[0].Key > key {
		if currentPage.Chld[0] == -1 {
			return "", fmt.Errorf("tree: Select Error: key %d not found", key)
		}
		t.table.Cursor = currentPage.Chld[0]
	} else if !IsNodeEmpty(currentPage.Data[MAX_KEYS-1]) && currentPage.Data[MAX_KEYS-1].Key < key {
		if currentPage.Chld[MAX_CHILDREN-1] == -1 {
			return "", fmt.Errorf("tree: Select Error: key %d not found", key)
		}
		t.table.Cursor = currentPage.Chld[MAX_CHILDREN-1]
	} else if currentPage.Data[0].Key == key {
		return ByteArr2String(currentPage.Data[0].Val), nil
	} else if currentPage.Data[MAX_KEYS-1].Key == key {
		return ByteArr2String(currentPage.Data[MAX_KEYS-1].Val), nil
	} else {

		for i := 1; i < MAX_KEYS; i++ {
			if IsNodeEmpty(currentPage.Data[i]) {
				return "", fmt.Errorf("tree: Select Error: key %d not found", key)
			}
			if currentPage.Data[i].Key > key && currentPage.Data[i-1].Key < key {
				if currentPage.Chld[i] == -1 {
					return "", fmt.Errorf("tree: Select Error: key %d not found", key)
				}
				t.table.Cursor = currentPage.Chld[i]
				break
			}
			if currentPage.Data[i].Key == key {
				return ByteArr2String(currentPage.Data[i].Val), nil
			}
		}
	}
	val, err := t.Select(key)
	if err != nil {
		return "", fmt.Errorf("tree: Select Error:%w", err)
	}
	return val, nil
}

func (t tree) Delete(key int32) error {
	return nil
}

func (t tree) Update(key int32, val string) error {
	return nil
}

func (t tree) SelectAll() error {

	// if table is empty
	if t.table.SrtOff == t.table.EndOff {
		return fmt.Errorf("tree: SelectAll Error: table is empty")
	}
	// if table is not empty, we need to select all from the tree
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("tree: SelectAll Error:%w", err)
	}
	currentPage := dsk.RecData.(TreePage)
	if currentPage.Head.IsLeaf {
		for _, v := range currentPage.Data {
			if IsNodeEmpty(v) {
				break
			}
			fmt.Printf("Key: %d, Value: %s\n", v.Key, v.Val)
		}
		return nil
	}
	for idx, chld := range currentPage.Chld {
		if chld == -1 {
			break
		}
		t.table.Cursor = chld
		err = t.SelectAll()
		if err != nil {
			return fmt.Errorf("TreeSelectAll Error:%w", err)
		}
		if idx == MAX_CHILDREN-1 {
			break
		}
		if IsNodeEmpty(currentPage.Data[idx]) {
			break
		}
		fmt.Printf("Key: %d, Value: %s\n", currentPage.Data[idx].Key, currentPage.Data[idx].Val)
	}
	return nil
}
