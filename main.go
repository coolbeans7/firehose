// firehose project main.go
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"strings"
)

var (
	queueDir = "/tmp/"
)

func traverseDirectories() {

	db, err := sql.Open("mysql", "root:everythingisawesome@localhost:3306/seeitremix")

	defer db.Close()

	tweetDirectories, err := ioutil.ReadDir(queueDir)
	if err != nil {
		os.Exit(1)
	}

	for _, tweetdirectory := range tweetDirectories {
		if tweetdirectory.IsDir() && strings.Contains(tweetdirectory.Name(), "tweet") {
			sparkfiles, err := ioutil.ReadDir(tweetdirectory.Name())
			for _, sparkfile := range sparkfiles {
				content, err := ioutil.ReadFile(sparkfile.Name())
				if err != nil {
					os.Exit(1)
				}
				lines := strings.Split(string(content), "\n")
				for _, sqlstmt := range lines {
					//db.Exec(sqlstmt)
					fmt.Print(sqlstmt)
				}
				if err != nil {
					os.Exit(1)
				}
			}
		}
	}
}

func main() {
	traverseDirectories()
}
