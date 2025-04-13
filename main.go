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
		switch inputBuff.inputString {
		case ".exit":
			os.Exit(0)
		default:
			fmt.Println("Unrecognised command")
		}
	}
}
