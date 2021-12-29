package run

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

var Db *sql.DB

func Server() {
	db, err := sql.Open("odbc", "DSN=Informix; DATABASE=rx21c")
	Check(err)
	Db = db

	MainLoop()
}

func Check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func Local() {
	db, err := sql.Open("mysql", "root:12345@tcp(localhost:3306)/mysql")
	Check(err)
	Db = db

	MainLoop()
}

func Ping() {
	err := Db.Ping()
	Check(err)
}

func MainLoop() {
	Ping()
	
	for {
		fmt.Print("q: ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString(byte('\t'))

		if strings.Trim(input, "\t\n ") == "end" {
			break
		}

		res, err := Db.Query(input)
		if err != nil {
			spew.Dump(err)
			continue
		}

		PrintJson(res)
	}
}

func PrintJson(rows *sql.Rows) {
	columns, err := rows.Columns()
	if err != nil {
		Check(err)
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}

	jsonData, err := json.MarshalIndent(tableData, "", "  ")
	if err != nil {
		Check(err)
	}

	fmt.Println(string(jsonData))
}
