package main

import (
	"bufio"
	statement "db/StatementManager"
	"fmt"
	"os"
	"strings"
)

type InpInfo struct {
	dbname string
	cmdStr string
}

func (i *InpInfo) printPrompt() {
	if i == nil {
		fmt.Printf("db>")
		return
	}
	fmt.Printf("db:%s>", i.dbname)
}

func (i *InpInfo) readInput() {

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading Input: ", err)
		os.Exit(1)
	}
	input = strings.TrimSpace(input)
	i.cmdStr = input
}

func main() {

	inpInfo := &InpInfo{}
	e := &statement.ExecutionInfo{}
	for {
		inpInfo.printPrompt()
		inpInfo.readInput()

		if inpInfo.cmdStr == "" {
			continue
		}

		if inpInfo.cmdStr[0] == '.' {
			err := statement.DoMetaCommand(inpInfo.cmdStr)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		}

		s := &statement.Statement{}
		err := s.PrepareStatement(inpInfo.cmdStr)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		e.StatementDetails = *s
		err = e.ExecuteStatement()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		if s.Cmd == statement.STATEMENT_DB_SWITCH {
			inpInfo.dbname = s.Inp.(statement.DBInfo).Name
		} else if s.Cmd == statement.STATEMENT_DB_DROPDB {
			inpInfo = &InpInfo{}
		}
	}
}
