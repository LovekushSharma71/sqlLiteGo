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
	PREPARE_SYNTAX_ERROR
)

const (
	STATEMENT_INSERT = iota
	STATEMENT_SELECT
)

const (
	EXECUTE_SUCCESS = iota
	EXECUTE_TABLE_FULL
	EXECUTE_INVALID_STATEMENT
	EXECUTE_INSERT_FAILED
)

type StatementType int

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

const (
	META_COMMAND_SUCCESS = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

func DoMetaCommand(inp string, table *Table) int {
	if inp == ".exit" {
		table.SyncTable2File()
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func (s *Statement) PrepareStatement(inpBuff InputBuffer) int {
	if inpBuff.inputLenght >= 6 && strings.ToLower(inpBuff.inputString[0:6]) == "insert" {
		s.Type = STATEMENT_INSERT

		args := strings.Split(inpBuff.inputString, " ")
		if len(args) < 4 {
			return PREPARE_SYNTAX_ERROR
		}

		id, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("Error id should be integer\nusage: insert id username email")
			return PREPARE_UNRECOGNIZED_STATEMENT
		}

		s.RowToInsert.Id = int32(id)
		if len(args[2]) > 255 || len(args[2]) == 0 {
			fmt.Println("Username(varchar(255)) cannot be of size:", len(args[2]))
			return PREPARE_SYNTAX_ERROR
		}
		copy(s.RowToInsert.Username[:], []byte(args[2]))

		if len(args[3]) > 255 || len(args[3]) == 0 {
			fmt.Println("Username(varchar(255)) cannot be of size:", len(args[2]))
			return PREPARE_SYNTAX_ERROR
		}
		copy(s.RowToInsert.Email[:], []byte(args[3]))

	} else if inpBuff.inputLenght >= 6 && strings.ToLower(inpBuff.inputString[0:6]) == "select" {
		s.Type = STATEMENT_SELECT
	} else {
		return PREPARE_UNRECOGNIZED_STATEMENT
	}
	return PREPARE_SUCCESS
}

func printRow(row *Row) {
	fmt.Printf("%d %s %s\n", row.Id, row.Username, row.Email)
}

func (s *Statement) ExecuteInsert(table *Table) int {
	if int(table.NumRows) >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}
	var rowToInsert *Row = &s.RowToInsert
	data, err := SerializeRow(rowToInsert)
	if err != nil {
		return EXECUTE_INSERT_FAILED
	}
	pageNum := table.NumRows / uint32(ROWS_PER_PAGE)
	rowNum := table.NumRows % uint32(ROWS_PER_PAGE)
	table.Pager.Pages[pageNum][rowNum] = data
	table.NumRows += 1
	return EXECUTE_SUCCESS
}

func (s *Statement) ExecuteSelect(table *Table) int {
	var row *Row
	// fmt.Println(table.NumRows)
	for i := uint32(0); i < table.NumRows; i++ {
		data, err := table.GetRowFromTable(i)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		row, err = DeserializeRow(data)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		printRow(row)
	}
	return EXECUTE_SUCCESS
}

func (s *Statement) ExecuteStatement(table *Table) int {
	switch s.Type {
	case STATEMENT_INSERT:
		// fmt.Println("Insert execution routine")
		return s.ExecuteInsert(table)
	case STATEMENT_SELECT:
		// fmt.Println("Select execution routine")
		return s.ExecuteSelect(table)
	}
	return EXECUTE_INVALID_STATEMENT
}
