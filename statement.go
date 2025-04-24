package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	PREPARE_SUCCESS = iota
	PREPARE_UNRECOGNIZED_STATEMENT
)

const (
	STATEMENT_INSERT = iota
	STATEMENT_SELECT
)

type StatementType int
type Row struct {
	Id       int
	Username string
	email    string
}
type Statement struct {
	Type        StatementType
	RowToInsert Row
}

const (
	META_COMMAND_SUCCESS = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

func DoMetaCommand(inp string) int {
	if inp == ".exit" {
		fmt.Println("")
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func (s *Statement) PrepareStatement(inpBuff InputBuffer) int {
	if inpBuff.inputLenght >= 6 && strings.ToLower(inpBuff.inputString[0:6]) == "insert" {
		s.Type = STATEMENT_INSERT
		args := strings.Split(inpBuff.inputString, " ")
		id, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("Error id should be integer\nusage: insert id username email")
			return PREPARE_UNRECOGNIZED_STATEMENT
		}
		s.RowToInsert.Id = id
		s.RowToInsert.Username = args[2]
		s.RowToInsert.email = args[3]

	} else if inpBuff.inputLenght >= 6 && strings.ToLower(inpBuff.inputString[0:6]) == "select" {
		s.Type = STATEMENT_SELECT
	} else {
		return PREPARE_UNRECOGNIZED_STATEMENT
	}
	return PREPARE_SUCCESS
}

func (s *Statement) ExecuteStatement() {
	switch s.Type {
	case STATEMENT_INSERT:
		fmt.Println("Insert execution routine")
	case STATEMENT_SELECT:
		fmt.Println("Select execution routine")
	}
}
