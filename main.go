package main

import "fmt"

// import (
// 	"bufio"
// 	"fmt"
// 	"os"
// 	"strings"
// )

// type InputBuffer struct {
// 	inputString string
// 	inputLenght int
// }

// func initInpBuff() *InputBuffer {

// 	inpBuff := &InputBuffer{}
// 	inpBuff.inputString = ""
// 	inpBuff.inputLenght = 0

// 	return inpBuff

// }

// func printPrompt() {
// 	fmt.Printf("db > ")
// }

// func (inp *InputBuffer) readInput() {

// 	reader := bufio.NewReader(os.Stdin)
// 	input, err := reader.ReadString('\n')
// 	if err != nil {
// 		fmt.Println("Error reading Input: ", err)
// 		os.Exit(1)
// 	}
// 	input = strings.TrimSpace(input)
// 	inp.inputString = input
// 	inp.inputLenght = len(input)
// }

// func main() {
// 	var inputBuff *InputBuffer = initInpBuff()
// 	var table *Table = NewTable()
// 	err := table.SyncFile2Table()
// 	if err != nil {
// 		os.Exit(0)
// 	}
// 	for {
// 		printPrompt()
// 		inputBuff.readInput()

// 		if inputBuff.inputString[0] == '.' {
// 			switch DoMetaCommand(inputBuff.inputString, table) {
// 			case META_COMMAND_SUCCESS:
// 				continue
// 			case META_COMMAND_UNRECOGNIZED_COMMAND:
// 				fmt.Printf("Unrecognized command '%s'.\n", inputBuff.inputString)
// 				continue
// 			}
// 		}

// 		statement := &Statement{}
// 		switch statement.PrepareStatement(*inputBuff) {
// 		case PREPARE_SUCCESS:
// 			// fmt.Println("prepare success")
// 		case PREPARE_SYNTAX_ERROR:
// 			fmt.Println("Syntax error: Could not parse statement")
// 			continue
// 		case PREPARE_UNRECOGNIZED_STATEMENT:
// 			fmt.Println("Invalid statement")
// 			continue
// 		}
// 		switch statement.ExecuteStatement(table) {
// 		case EXECUTE_SUCCESS:
// 			fmt.Println("Executed")
// 		case EXECUTE_TABLE_FULL:
// 			fmt.Println("Error: Table full")
// 		case EXECUTE_INVALID_STATEMENT:
// 			fmt.Println("Error: Invalid statement")
// 		}
// 	}
// }

func main() {
	node := []Node{
		{1, [255]byte{'1'}},
		{3, [255]byte{'1'}},
		{6, [255]byte{'1'}},
		{9, [255]byte{'1'}},
		{14, [255]byte{'1'}},
		{16, [255]byte{'1'}},
	}
	fmt.Println(Search(node, -1), Search(node, 2), Search(node, 4), Search(node, 7), Search(node, 15), Search(node, 17))
	table := InitDisk()
	pge := Page{
		IsLeaf:  true,
		NodeCnt: 2,
		Nodes:   [3]Node{{1, [255]byte{'v', 'a', 'l', '1'}}, {2, [255]byte{'v', 'a', 'l', '2'}}},
		// Nodes:    [3]Node{{1, "[255]byte{'v', 'a', 'l', '1'}"}, {2, "[255]byte{'v', 'a', 'l', '2'}"}},
		Children: [4]PageId{-1, -1, -1},
	}
	if err := table.wrtPage(0, &pge); err != nil {
		fmt.Println("error in write")
		panic(err)
	}
	pge = Page{
		IsLeaf:  true,
		NodeCnt: 2,
		Nodes:   [3]Node{{3, [255]byte{'v', 'a', 'l', '1'}}, {4, [255]byte{'v', 'a', 'l', '2'}}},
		// Nodes:    [3]Node{{1, "[255]byte{'v', 'a', 'l', '1'}"}, {2, "[255]byte{'v', 'a', 'l', '2'}"}},
		Children: [4]PageId{-1, -1, -1},
	}
	if err := table.wrtPage(1, &pge); err != nil {
		fmt.Println("error in write")
		panic(err)
	}
	page, err := table.getPage(0)
	if err != nil {
		fmt.Println("error in read")
		panic(err)
	}
	fmt.Printf("%v %v %v %v\n", page.IsLeaf, page.NodeCnt, page.Children, page.Nodes)
	page, err = table.getPage(1)
	if err != nil {
		fmt.Println("error in read")
		panic(err)
	}
	fmt.Printf("%v %v %v %v\n", page.IsLeaf, page.NodeCnt, page.Children, page.Nodes)
	root, err := table.getPage(0)
	if err != nil {
		panic(err)
	}
	fmt.Println(table.Select(root, 1))
	fmt.Println(table.Select(root, 5))
}
