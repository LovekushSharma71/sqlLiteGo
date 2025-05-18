package main

import diskmanager "db/DiskManager"

func main() {
	t, err := diskmanager.InitDiskManager("test", diskmanager.DT_LIST_PAGE)
	if err != nil {
		panic(err)
	}

	err = t.Insert(1, "val1")
	if err != nil {
		panic(err)
	}
	err = t.SelectAll()
	if err != nil {
		panic(err)
	}
}

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

// func main() {

// dsk, err := dm.InitDiskManager("test")
// if err != nil {
// 	panic(err)
// }
// defer dsk.Close()

// t.Insert(1, "val1")
// t.Select()

// t.Insert(2, "val2")
// t.Select()

// }
