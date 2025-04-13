package main

import (
	"fmt"
	"os"
)

/*
typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT } PrepareResult;
+
+typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;
+
+typedef struct {
+  StatementType type;
+} Statement;
*/

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
