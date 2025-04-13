package main

import (
	"fmt"
	"os"
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

type Statement struct {
	Type StatementType
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
	switch strings.ToLower(inpBuff.inputString[0:6]) {
	case "insert":
		s.Type = STATEMENT_INSERT
	case "select":
		s.Type = STATEMENT_SELECT
	default:
		return PREPARE_UNRECOGNIZED_STATEMENT
	}
	return PREPARE_SUCCESS
}
