// firehose project main.go
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var (
	queueDir = "/tmp/"
)

func traverseDirectories() {

	db, err := sql.Open("mysql", "root:everythingisawesome@localhost:3306/seeitremix")
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	defer db.Close()

	tweetDirectories, _ := ioutil.ReadDir(queueDir)

	for _, tweetdirectory := range tweetDirectories {
		if tweetdirectory.IsDir() && strings.Contains(tweetdirectory.Name(), "tweet") {
			fmt.Print(filepath.Join(queueDir, tweetdirectory.Name()) + "\n")
			sparkfiles, _ := ioutil.ReadDir(filepath.Join(queueDir, tweetdirectory.Name()))
			for _, sparkfile := range sparkfiles {
				fmt.Print(filepath.Join(queueDir, tweetdirectory.Name(), sparkfile.Name()) + "\n")
				content, err := ioutil.ReadFile(filepath.Join(queueDir, tweetdirectory.Name(), sparkfile.Name()))
				if err != nil {
					fmt.Print(err)
					os.Exit(1)
				}
				lines := strings.Split(string(content), "\n")
				for _, sqlstmt := range lines {
					fmt.Print(sqlstmt)
					_, sterr := db.Exec(sqlstmt)
					if sterr != nil {
						fmt.Print(sterr)
						os.Exit(1)
					}

				}
			}
		}
	}
}

func main() {
	traverseDirectories()
}
