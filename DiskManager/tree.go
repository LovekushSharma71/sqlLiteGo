package diskmanager

import (
	"errors"
	"fmt"
	"syscall"
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
		root, err := t.table.WrtDiskData(TreePage{
			Head: TreeHead{
				IsLeaf: true,
				IsRoot: true,
				Parent: -1,
			},
			Data: [MAX_KEYS]DataNode{{Key: key, Val: String2ByteArr(val)}},
			Chld: [MAX_CHILDREN]int32{},
		})
		if err != nil {
			return fmt.Errorf("tree: Insert (empty tree WrtDiskData): %w", err)
		}
		err = t.table.WrtDBHeader(TableHeader{
			RootAddr: root.RecHead.RecAddr,
			IsLinear: false,
		})
		if err != nil {
			return fmt.Errorf("tree: Insert (empty tree WrtDiskData): %w", err)
		}
		t.table.SrtOff = root.RecHead.RecAddr
		t.table.Cursor = root.RecHead.RecAddr // Set cursor to the new root
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
		return nil
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
		return "", fmt.Errorf("tree: Select Error: table is empty")
	}
	// if table is not empty, we need to select all from the tree
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return "", fmt.Errorf("tree: Select Error:%w", err)
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
	for i := 0; i < MAX_KEYS; i++ {
		if IsNodeEmpty(currentPage.Data[i]) {
			if currentPage.Chld[i] == -1 || currentPage.Chld[i] == 0 {
				return "", fmt.Errorf("tree: Select Error: key %d not found", key)
			}
			t.table.Cursor = currentPage.Chld[i]
			return t.Select(key)
		}
		if currentPage.Data[i].Key == key {
			return ByteArr2String(currentPage.Data[i].Val), nil
		}
		if currentPage.Data[i].Key > key {
			if currentPage.Chld[i] == -1 || currentPage.Chld[i] == 0 {
				return "", fmt.Errorf("tree: Select Error: key %d not found", key)
			}
			t.table.Cursor = currentPage.Chld[i]
			return t.Select(key)
		}
	}
	numValidKeysInNode := 0
	for _, v_key := range currentPage.Data {
		if IsNodeEmpty(v_key) {
			break
		}
		numValidKeysInNode++
	}
	if currentPage.Chld[numValidKeysInNode] == 0 || currentPage.Chld[numValidKeysInNode] == -1 {
		return "", fmt.Errorf("tree: Select Error: key %d not found (no rightmost child path)", key)
	}
	t.table.Cursor = currentPage.Chld[numValidKeysInNode]
	return t.Select(key)
}

type DeleteKeyError struct {
	isUnderfull bool       // Indicates if the node is underfull after deletion
	UpdatedNode []DataNode // The node that was updated or demoted
	Err         error
}

func (e *DeleteKeyError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("node is underfull after deletion, %t:%s", e.isUnderfull, e.Error())
	}
	return fmt.Sprintf("node is underfull after deletion, %t", e.isUnderfull)
}

// Unwrap allows this error to be unwrapped to reveal the underlying error
func (e *DeleteKeyError) Unwrap() error {
	return e.Err
}

func (t tree) DeleteLeafNode(currentPage TreePage, key int32) error {

	delIdx := -1
	numCurrentKeys := 0
	for i, v := range currentPage.Data {
		if IsNodeEmpty(v) {
			break
		}
		if v.Key == key {
			delIdx = i
		}
		numCurrentKeys++
	}
	if delIdx == -1 {
		return fmt.Errorf("DeleteLeafNode Error: key %d not found", key)
	}
	NodeBuf := make([]DataNode, numCurrentKeys-1)
	copy(NodeBuf, currentPage.Data[:delIdx])
	copy(NodeBuf[delIdx:], currentPage.Data[delIdx+1:numCurrentKeys])
	currentPage.Data = [MAX_KEYS]DataNode{}
	copy(currentPage.Data[:], NodeBuf)
	if len(NodeBuf) < MIN_KEYS {
		return &DeleteKeyError{
			isUnderfull: true,
			UpdatedNode: NodeBuf,
			Err:         fmt.Errorf("DeleteLeafNode Error: node is underfull after deletion, key %d", key),
		}
	}
	err := t.table.EdtDiskData(currentPage)
	if err != nil {
		return fmt.Errorf("DeleteLeafNode error: %w", err)
	}
	return nil // TODO: implement actual leaf removal, handle
}

func (t tree) DeleteNode(currentPage TreePage, key int32) error {

	delidx := -1
	numCurrentKeys := 0
	for i, v := range currentPage.Data {
		if IsNodeEmpty(v) {
			break
		}
		if v.Key == key {
			delidx = i
		}
		numCurrentKeys++
	}
	if delidx == -1 {
		return fmt.Errorf("DeleteNode Error: key %d not found", key)
	}

	nodeBuf := make([]DataNode, numCurrentKeys)
	chldBuf := make([]int32, numCurrentKeys+1)
	copy(nodeBuf, currentPage.Data[:delidx])
	copy(nodeBuf[delidx:], currentPage.Data[delidx+1:numCurrentKeys])
	copy(chldBuf, currentPage.Chld[:delidx+1])
	copy(chldBuf[delidx:], currentPage.Chld[delidx+1:numCurrentKeys+1])
	if len(nodeBuf) < MIN_KEYS {
		return &DeleteKeyError{
			isUnderfull: true,
			UpdatedNode: nodeBuf,
			Err:         fmt.Errorf("DeleteNode Error: node is underfull after deletion, key %d", key),
		}
	}
	currentPage.Data = [MAX_KEYS]DataNode{}
	copy(currentPage.Data[:], nodeBuf)
	currentPage.Chld = [MAX_CHILDREN]int32{}
	copy(currentPage.Chld[:], chldBuf)
	err := t.table.EdtDiskData(currentPage)
	if err != nil {
		return fmt.Errorf("DeleteNode error: %w", err)
	}
	return nil
}

func (t tree) Borrow(currPageAddr int32, idx int) error {

	if currPageAddr == -1 || currPageAddr == 0 {
		return fmt.Errorf("Borrow error: invalid address")
	}
	t.table.Cursor = currPageAddr
	currDsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("Borrow error:%w", err)
	}
	currPage := currDsk.RecData.(TreePage)

	ChldAddr := currPage.Chld[idx]
	if ChldAddr == -1 || ChldAddr == 0 {
		return fmt.Errorf("Borrow error: invalid address")
	}
	t.table.Cursor = ChldAddr
	chldDsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("Borrow error:%w", err)
	}
	chldPage := chldDsk.RecData.(TreePage)

	leftChldAddr := currPage.Chld[idx-1]
	if leftChldAddr == -1 || leftChldAddr == 0 {
		return fmt.Errorf("Borrow error: invalid address")
	}
	t.table.Cursor = leftChldAddr
	leftChldDsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("Borrow error:%w", err)
	}
	leftChldPage := leftChldDsk.RecData.(TreePage)

	rightChldAddr := currPage.Chld[idx+1]
	if rightChldAddr == -1 || rightChldAddr == 0 {
		return fmt.Errorf("Borrow error: invalid address")
	}
	t.table.Cursor = rightChldAddr
	rightChldDsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("Borrow error:%w", err)
	}
	rightChldPage := rightChldDsk.RecData.(TreePage)

	numLeftKeys := -1
	for i, v := range leftChldPage.Data {
		if IsNodeEmpty(v) {
			numLeftKeys = i
			break
		}
	}
	if numLeftKeys > MIN_KEYS {
		leftMax := leftChldPage.Data[numLeftKeys-1]
		leftChldPage.Data[numLeftKeys-1] = DataNode{}
		curr := currPage.Data[idx]
		currPage.Data[idx] = leftMax
		copy(chldPage.Data[1:], chldPage.Data[:])
		chldPage.Data[0] = curr

		t.table.Cursor = currPageAddr
		err = t.table.EdtDiskData(currPage)
		if err != nil {
			return fmt.Errorf("Borrow error: %w", err)
		}

		t.table.Cursor = leftChldAddr
		err = t.table.EdtDiskData(leftChldPage)
		if err != nil {
			return fmt.Errorf("Borrow error: %w", err)
		}

		t.table.Cursor = ChldAddr
		err = t.table.EdtDiskData(chldPage)
		if err != nil {
			return fmt.Errorf("Borrow error: %w", err)
		}
		return nil
	}
	numRightKeys := -1
	for i, v := range leftChldPage.Data {
		if IsNodeEmpty(v) {
			numRightKeys = i
			break
		}
	}
	if numRightKeys > MIN_KEYS {
		rightMax := rightChldPage.Data[0]
		rightChldPage.Data[0] = DataNode{}
		curr := currPage.Data[idx]
		currPage.Data[idx] = rightMax

		// can use MIN_KEYS+! but just in case
		numChldKeys := -1
		for i, v := range currPage.Data {
			if IsNodeEmpty(v) {
				numChldKeys = i
				break
			}
		}
		chldPage.Data[numChldKeys] = curr

		t.table.Cursor = currPageAddr
		err = t.table.EdtDiskData(currPage)
		if err != nil {
			return fmt.Errorf("Borrow error: %w", err)
		}

		t.table.Cursor = rightChldAddr
		err = t.table.EdtDiskData(leftChldPage)
		if err != nil {
			return fmt.Errorf("Borrow error: %w", err)
		}

		t.table.Cursor = ChldAddr
		err = t.table.EdtDiskData(chldPage)
		if err != nil {
			return fmt.Errorf("Borrow error: %w", err)
		}
		return nil
	}
	t.table.Cursor = currPageAddr
	// using syscall.EINVAL because dont want to create a custom error
	return fmt.Errorf("Borrow error: %w", syscall.EINVAL)
}

func (t tree) Merge() error {
	return nil
}

func (t tree) Delete(key int32) error {

	// if table is empty
	if t.table.SrtOff == t.table.EndOff {
		return fmt.Errorf("tree: Delete Error: table is empty")
	}
	// if table is not empty, we need to select all from the tree
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("tree: Delete Error:%w", err)
	}
	currentPage := dsk.RecData.(TreePage)
	currentPageAddr := dsk.RecHead.RecAddr
	if currentPage.Head.IsLeaf {
		for _, v := range currentPage.Data {
			if IsNodeEmpty(v) {
				break
			}
			if v.Key == key {
				err = t.DeleteLeafNode(currentPage, key)
				if err != nil {
					return fmt.Errorf("tree: Delete Error:%w", err)
				}
				return nil
			}
		}
		return fmt.Errorf("tree: Delete Error: key %d not found", key)
	}
	for i := 0; i < MAX_KEYS; i++ {
		if IsNodeEmpty(currentPage.Data[i]) {
			if currentPage.Chld[i] == -1 || currentPage.Chld[i] == 0 {
				return fmt.Errorf("tree: Update Error: key %d not found", key)
			}
			t.table.Cursor = currentPage.Chld[i]
			err = t.Delete(key)
			if errors.As(err, new(*DeleteKeyError)) {
				err = t.Borrow(currentPageAddr, i)
				if err != nil {
					if err == syscall.EINVAL {
						err = t.Merge()
						if err != nil {
							return fmt.Errorf("tree: Delete error:%w", err)
						}
					}
					return fmt.Errorf("tree: Delete error:%w", err)
				}
				return nil
			}
			if err != nil {
				return fmt.Errorf("tree: Delete Error:%w", err)
			}
			return nil
		}
		if currentPage.Data[i].Key == key {
			err = t.DeleteNode(currentPage, key)
			if err != nil {
				return fmt.Errorf("tree: Delete Error:%w", err)
			}
			return nil
		}
		if currentPage.Data[i].Key > key {
			if currentPage.Chld[i] == -1 || currentPage.Chld[i] == 0 {
				return fmt.Errorf("tree: Delete Error: key %d not found", key)
			}
			t.table.Cursor = currentPage.Chld[i]
			err = t.Delete(key)
			if errors.As(err, new(*DeleteKeyError)) {
				// If we get a DeleteKeyError, it means we need to demote a key from the child node
				err = t.Borrow(currentPageAddr, i)
				if err != nil {
					if err == syscall.EINVAL {
						err = t.Merge()
						if err != nil {
							return fmt.Errorf("tree: Delete Error:%w", err)
						}
					}
					return fmt.Errorf("tree: Delete Error:%w", err)
				}
			} else if err != nil {
				return fmt.Errorf("tree: Delete Error:%w", err)
			}
			return nil
		}
	}
	numValidKeysInNode := 0
	for _, v_key := range currentPage.Data {
		if IsNodeEmpty(v_key) {
			break
		}
		numValidKeysInNode++
	}
	if currentPage.Chld[numValidKeysInNode] == 0 || currentPage.Chld[numValidKeysInNode] == -1 {
		return fmt.Errorf("tree: Delete Error: key %d not found (no rightmost child path)", key)
	}
	t.table.Cursor = currentPage.Chld[numValidKeysInNode]
	err = t.Delete(key)
	if errors.As(err, new(*DeleteKeyError)) {
		// If we get a DeleteKeyError, it means we need to demote a key from the child node
		err = t.Borrow(currentPageAddr, numValidKeysInNode)
		if err != nil {
			if err == syscall.EINVAL {
				err = t.Merge()
				if err != nil {
					return fmt.Errorf("tree: Delete Error:%w", err)
				}
			}
			return fmt.Errorf("tree: Delete Error:%w", err)
		}
	} else if err != nil {
		return fmt.Errorf("tree: Delete Error:%w", err)
	}
	return nil
}

func (t tree) Update(key int32, val string) error {

	if len(val) > 32 {
		return fmt.Errorf("tree: Update Error: val size length is greater than 32")
	}
	// if table is empty
	if t.table.SrtOff == t.table.EndOff {
		return fmt.Errorf("tree: Update Error: table is empty")
	}
	buf := String2ByteArr(val)
	// if table is not empty, we need to select all from the tree
	dsk, err := t.table.GetDiskData()
	if err != nil {
		return fmt.Errorf("tree: Update Error:%w", err)
	}
	currentPage := dsk.RecData.(TreePage)
	if currentPage.Head.IsLeaf {
		for i, v := range currentPage.Data {
			if IsNodeEmpty(v) {
				break
			}
			if v.Key == key {
				currentPage.Data[i].Val = buf
				err = t.table.EdtDiskData(currentPage)
				if err != nil {
					return fmt.Errorf("tree: Update Error:%w", err)
				}
				return nil
			}
		}
		return fmt.Errorf("tree: Update Error: key %d not found", key)
	}
	for i := 0; i < MAX_KEYS; i++ {
		if IsNodeEmpty(currentPage.Data[i]) {
			if currentPage.Chld[i] == -1 || currentPage.Chld[i] == 0 {
				return fmt.Errorf("tree: Update Error: key %d not found", key)
			}
			t.table.Cursor = currentPage.Chld[i]
			return t.Update(key, val)
		}
		if currentPage.Data[i].Key == key {
			currentPage.Data[i].Val = buf
			err = t.table.EdtDiskData(currentPage)
			if err != nil {
				return fmt.Errorf("tree: Update Error:%w", err)
			}
			return nil
		}
		if currentPage.Data[i].Key > key {
			if currentPage.Chld[i] == -1 || currentPage.Chld[i] == 0 {
				return fmt.Errorf("tree: Update Error: key %d not found", key)
			}
			t.table.Cursor = currentPage.Chld[i]
			return t.Update(key, val)
		}
	}
	numValidKeysInNode := 0
	for _, v_key := range currentPage.Data {
		if IsNodeEmpty(v_key) {
			break
		}
		numValidKeysInNode++
	}
	if currentPage.Chld[numValidKeysInNode] == 0 || currentPage.Chld[numValidKeysInNode] == -1 {
		return fmt.Errorf("tree: Update Error: key %d not found (no rightmost child path)", key)
	}
	t.table.Cursor = currentPage.Chld[numValidKeysInNode]
	return t.Update(key, val)
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
		if chld == -1 || chld == 0 {
			break
		}
		t.table.Cursor = chld
		err = t.SelectAll()
		if err != nil {
			return fmt.Errorf("tree: SelectAll Error:%w", err)
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
