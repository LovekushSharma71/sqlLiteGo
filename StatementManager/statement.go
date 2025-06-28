package statement

import (
	diskmanager "db/DiskManager"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	STATEMENT_DB_INSERT = iota
	STATEMENT_DB_SELECT
	STATEMENT_DB_UPDATE
	STATEMENT_DB_DELETE
	STATEMENT_DB_CREATE
	STATEMENT_DB_DROPDB
	STATEMENT_DB_SWITCH
)

type StatementType int
type Table diskmanager.Table

type KV struct {
	Key int32
	Val string
}

type DBInfo struct {
	Name string
	Type string
}

type Statement struct {
	Cmd StatementType
	Inp interface{}
}

type ExecutionInfo struct {
	StatementDetails Statement
	TableDetails     diskmanager.Table
}

func DoMetaCommand(cmd string) error {
	if cmd == ".exit" {
		os.Exit(0)
	}
	return fmt.Errorf("unrecognised meta command: %s", cmd)
}

func (s *Statement) PrepareStatement(inpBuf string) error {

	if len(inpBuf) < 6 {
		return fmt.Errorf("statement error: invalid statement %s", inpBuf)
	}

	switch cmd := inpBuf[:6]; strings.ToLower(cmd) {
	case "insert":
		s.Cmd = STATEMENT_DB_INSERT
		args := strings.Split(inpBuf, " ")
		if len(args) != 3 {
			return fmt.Errorf("statement error: syntax error\n ussage: insert key value")
		}
		key, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("statement error: invalid key provided %w", err)
		}
		if len(args[2]) > 32 {
			return fmt.Errorf("statement error: string length cannot exceed 32 got %d", len(args[2]))
		}
		s.Inp = KV{
			Key: int32(key),
			Val: args[2],
		}
	case "select":
		s.Cmd = STATEMENT_DB_SELECT
		args := strings.Split(inpBuf, " ")
		if len(args) != 2 {
			return fmt.Errorf("statement error: syntax error\n ussage: select key or select all")
		}
		if args[1] == "all" {
			s.Inp = "all"
		} else {
			key, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("statement error: invalid key provided %w", err)
			}
			s.Inp = KV{
				Key: int32(key),
				Val: "",
			}
		}
	case "update":
		s.Cmd = STATEMENT_DB_UPDATE
		args := strings.Split(inpBuf, " ")
		if len(args) != 3 {
			return fmt.Errorf("statement error: syntax error\n ussage: update key value")
		}
		key, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("statement error: invalid key provided %w", err)
		}
		if len(args[2]) >= 32 {
			return fmt.Errorf("statement error: string length cannot exceed 32 got %d", len(args[2]))
		}
		s.Inp = KV{
			Key: int32(key),
			Val: args[2],
		}
	case "delete":
		s.Cmd = STATEMENT_DB_DELETE
		args := strings.Split(inpBuf, " ")
		if len(args) != 2 {
			return fmt.Errorf("statement error: syntax error\n ussage: delete key")
		}
		key, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("statement error: invalid key provided %w", err)
		}
		s.Inp = KV{
			Key: int32(key),
			Val: "",
		}
	case "create":
		s.Cmd = STATEMENT_DB_CREATE
		args := strings.Split(inpBuf, " ")
		if len(args) != 3 {
			return fmt.Errorf("statement error: syntax error\n ussage: create dbname tabletype")
		}
		if len(args[1]) >= 32 {
			return fmt.Errorf("statement error: database name length should not exceed 32")
		}
		allowedDBType := []string{"tree", "list"}
		isValid := false
		for _, v := range allowedDBType {
			if strings.ToLower(args[2]) == v {
				s.Inp = DBInfo{
					Name: args[1],
					Type: v,
				}
				isValid = true
			}
		}
		if !isValid {
			return fmt.Errorf("statement error: invalid tree type allowed db table types are %v", allowedDBType)
		}
	case "dropdb":
		s.Cmd = STATEMENT_DB_DROPDB
		args := strings.Split(inpBuf, " ")
		if len(args) != 2 {
			return fmt.Errorf("statement error: syntax error\n ussage: dropdb dbname")
		}
		if len(args[1]) >= 32 {
			return fmt.Errorf("statement error: database name length should not exceed 32")
		}
		s.Inp = DBInfo{
			Name: args[1],
		}
	case "switch":
		s.Cmd = STATEMENT_DB_SWITCH
		args := strings.Split(inpBuf, " ")
		if len(args) != 2 {
			return fmt.Errorf("statement error: syntax error\n ussage: drop dbname")
		}
		if len(args[1]) >= 32 {
			return fmt.Errorf("statement error: database name length should not exceed 32")
		}
		s.Inp = DBInfo{
			Name: args[1],
		}
	default:
		return fmt.Errorf("statemnet error: invalid command %s", cmd)
	}
	return nil
}

func (e *ExecutionInfo) ExecuteStatement() error {
	if e == nil {
		return fmt.Errorf("execute error: nil execution info error")
	}
	defer func() error {
		err := e.TableDetails.ResetCursor()
		if err != nil {
			return err
		}
		return nil
	}()
	switch e.StatementDetails.Cmd {
	case STATEMENT_DB_INSERT:
		if e.TableDetails == nil {
			return fmt.Errorf("execute error: nil table, select table")
		}
		kv := e.StatementDetails.Inp.(KV)
		err := e.TableDetails.Insert(kv.Key, kv.Val)
		if err != nil {
			return fmt.Errorf("execute error: %w", err)
		}
		fmt.Println("execute success: insert")
	case STATEMENT_DB_SELECT:
		if e.TableDetails == nil {
			return fmt.Errorf("execute error: nil table, select table")
		}
		inp, ok := e.StatementDetails.Inp.(KV)
		if !ok {
			if e.StatementDetails.Inp.(string) != "all" {
				return fmt.Errorf("execute error: invalid select input %v", e.StatementDetails.Inp)
			}
			err := e.TableDetails.SelectAll()
			if err != nil {
				return fmt.Errorf("execute error:%w", err)
			}
			return nil
		}
		val, err := e.TableDetails.Select(inp.Key)
		if err != nil {
			return fmt.Errorf("execute error:%w", err)
		}
		fmt.Printf("output- Key:%d Value:%s\n", inp.Key, val)
	case STATEMENT_DB_UPDATE:
		if e.TableDetails == nil {
			return fmt.Errorf("execute error: nil table, select table")
		}
		kv := e.StatementDetails.Inp.(KV)
		err := e.TableDetails.Update(kv.Key, kv.Val)
		if err != nil {
			return fmt.Errorf("execute error: %w", err)
		}
		fmt.Println("execute success: update")
	case STATEMENT_DB_DELETE:
		if e.TableDetails == nil {
			return fmt.Errorf("execute error: nil table, select table")
		}
		kv := e.StatementDetails.Inp.(KV)
		err := e.TableDetails.Delete(kv.Key)
		if err != nil {
			return fmt.Errorf("execute error: %w", err)
		}
		fmt.Println("execute success: delete")
	case STATEMENT_DB_CREATE:
		info := e.StatementDetails.Inp.(DBInfo)
		err := diskmanager.CreateDatabase(info.Name, info.Type)
		if err != nil {
			return fmt.Errorf("execute error: %w", err)
		}
		fmt.Println("execute success: create")
	case STATEMENT_DB_SWITCH:
		info := e.StatementDetails.Inp.(DBInfo)
		dsk, err := diskmanager.InitDatabase(info.Name)
		if err != nil {
			return fmt.Errorf("execute error: %w", err)
		}
		e.TableDetails = diskmanager.InitTable(dsk)
		fmt.Println("execute success: switched to database: ", info.Name)
	case STATEMENT_DB_DROPDB:
		info := e.StatementDetails.Inp.(DBInfo)
		err := diskmanager.DropDatabase(info.Name)
		if err != nil {
			return fmt.Errorf("execution error: %w", err)
		}
	default:
		return fmt.Errorf("unrecognised command")
	}
	return nil
}
