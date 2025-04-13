package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type InputBuffer struct {
	inputString string
	inputLenght int
}

func initInpBuff() *InputBuffer {

	inpBuff := &InputBuffer{}
	inpBuff.inputString = ""
	inpBuff.inputLenght = 0

	return inpBuff

}

func printPrompt() {
	fmt.Printf("db > ")
}

func (inp *InputBuffer) readInput() {

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading Input: ", err)
		os.Exit(1)
	}
	input = strings.TrimSpace(input)
	inp.inputString = input
	inp.inputLenght = len(input)
}

func main() {
	var inputBuff *InputBuffer = initInpBuff()
	for {
		printPrompt()
		inputBuff.readInput()

		if inputBuff.inputString[0] == '.' {
			switch DoMetaCommand(inputBuff.inputString) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", inputBuff.inputString)
				continue
			}
		}

		statement := &Statement{}
		switch statement.PrepareStatement(*inputBuff) {
		case PREPARE_SUCCESS:
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Println("Invalid statement")

		}

	}
}
