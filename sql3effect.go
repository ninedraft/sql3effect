package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/olekukonko/tablewriter"
)

const help = `
SQLite3 query and exec multitool.

-exec and -query calls are executed on one transaction.

Example invocations:

-query "SELECT name FROM users WHERE id=@user_id" -arg user_id=100500:integer  -exec "DELETE FROM users WHERE name like ?" -arg '%spam%'


The following SQLite3 exts are compiled in:
    math functions
    FTS5
    JSON
    R*Tree
    GeoPoly
    Spellfix1
    soundex
    stat4
    base64
    decimal
    ieee754
    regexp
    series
    uint
    time
		`

type sqlCall struct {
	query, exec string
	args        []any
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var calls []sqlCall

	flag.Func("query", "query expressions", func(q string) error {
		if strings.TrimSpace(q) != "" {
			calls = append(calls, sqlCall{
				query: q,
			})
		}
		return nil
	})

	flag.Func("exec", "exec expressions", func(e string) error {
		if strings.TrimSpace(e) != "" {
			calls = append(calls, sqlCall{
				exec: e,
			})
		}
		return nil
	})

	supportedArgTypes := []string{"null", "integer", "real", "text", "blob"}
	flag.Func("arg",
		"SQL argument with optional name and type. Examples: 10, count=10, 10:integer, count=10:integer, null \n"+
			"If name is not defined, then ordinal position of argument will be used in query\n"+
			"Supported types: "+strings.Join(supportedArgTypes, ", "),
		func(argument string) error {
			name, argument, ok := strings.Cut(argument, "=")
			if !ok {
				argument = name
				name = ""
			}

			argument, argType, ok := strings.Cut(argument, ":")
			switch {
			case !ok && argument != "null":
				argType = "text"
			case !ok && argument == "null":
				argType = "null"
			}

			argValue := any(argument)
			var err error
			switch argType {
			case "null":
				argValue = nil
			case "integer":
				argValue, err = strconv.ParseInt(argument, 0, 64)
			case "real":
				argValue, err = strconv.ParseFloat(argument, 64)
			case "text", "blob":
				// pass
			default:
				return fmt.Errorf("unknown argument type %q", argType)
			}

			if err != nil {
				return fmt.Errorf("parsing SQL argument: %w", err)
			}

			if len(calls) == 0 {
				return errors.New("-arg is set before -query or -exec - can't set argument. Use like following: -query `select $1` -arg 10:integer")
			}

			latestCall := &calls[len(calls)-1]
			latestCall.args = append(latestCall.args, sql.Named(name, argValue))

			return nil
		})

	flag.BoolFunc("list", "list tables from database", func(string) error {
		calls = append(calls, sqlCall{
			query: "SELECT name FROM sqlite_master WHERE type='table'",
		})
		return nil
	})

	dbFile := ""
	flag.StringVar(&dbFile, "db", dbFile, "Database file to use")

	flag.Usage = func() {
		fmt.Println(help)
		flag.PrintDefaults()
	}

	flag.Parse()

	if strings.TrimSpace(dbFile) == "" {
		flag.Usage()
		panic("no database file specified")
	}

	db, err := sql.Open("sqlite3", "file:"+dbFile)
	if err != nil {
		panic("database open:" + err.Error())
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		panic("opening connections: " + err.Error())
	}
	defer tx.Rollback()

	for _, call := range calls {
		switch {
		case call.exec != "":
			fmt.Println(">", call.exec)
			result, err := tx.ExecContext(ctx, call.exec, call.args...)
			if err != nil {
				panic("exec: " + err.Error())
			}
			fmt.Printf("rows affected: ")
			fmt.Println(result.RowsAffected())
		case call.query != "":
			if err := runQuery(ctx, tx, call.query, call.args); err != nil {
				panic(err)
			}
		default:
			panic("bad SQL call without query or exec")
		}
	}

	if err := tx.Commit(); err != nil {
		panic("commit: " + err.Error())
	}
}

func runQuery(ctx context.Context, db *sql.Tx, query string, args []any) error {
	fmt.Println("> ", query)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if len(columns) == 0 {
		return nil
	}

	tw := tablewriter.NewWriter(os.Stdout)
	tw.Header(columns)

	row := make([]any, len(columns))
	rowValues := make([]any, len(columns))
	for i := range row {
		rowValues[i] = &row[i]
	}

	for rows.Next() {
		if err := rows.Scan(rowValues...); err != nil {
			return fmt.Errorf("reading result %w", err)
		}

		tw.Append(row)
	}

	return errors.Join(
		rows.Err(),
		tw.Render(),
	)
}
