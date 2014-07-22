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
    "time"
    "flag"
)

var (
	queueDir = "/tmp/"
    scanInterval time.Duration
)

func traverseDirectories() {

	db, err := sql.Open("mysql", "root:everythingisawesome@tcp(localhost:3306)/seeitremix")
	if err != nil {
        fmt.Print("connection error: ")
		fmt.Print(err)
		os.Exit(1)
	}

	defer db.Close()

for {
	tweetDirectories, _ := ioutil.ReadDir(queueDir)

	for _, tweetdirectory := range tweetDirectories {
		if tweetdirectory.IsDir() && strings.Contains(tweetdirectory.Name(), "tweet") {
			sparkfiles, _ := ioutil.ReadDir(filepath.Join(queueDir, tweetdirectory.Name()))
			for _, sparkfile := range sparkfiles {
                if tweetdirectory.IsDir() && strings.Contains(sparkfile.Name(), "part") && !strings.HasPrefix(sparkfile.Name(), ".") {
				content, err := ioutil.ReadFile(filepath.Join(queueDir, tweetdirectory.Name(), sparkfile.Name()))
				if err != nil {
					fmt.Print(err)
					os.Exit(1)
				}
				lines := strings.Split(string(content), "\n")
				for _, sqlstmt := range lines {
                    if strings.Contains(sqlstmt, "INSERT") {
					_, sterr := db.Exec(sqlstmt)
					if sterr != nil {
                        fmt.Print("statement error=", sterr, "\n")
                        fmt.Print("sql from file: ", filepath.Join(queueDir, tweetdirectory.Name(), sparkfile.Name()), "\n")
                        fmt.Print("sql stmt: ", sqlstmt, "\n")
					}
                  }
				}
			}
        }
        fmt.Print("Action=RemoveDir, Directory=", tweetdirectory.Name())
		}
	}
    time.Sleep(scanInterval)
    fmt.Print("Done\n") }
}

func main() {
    flag.DurationVar(&scanInterval, "i", time.Duration(5*time.Second), "scan interval")
    flag.Parse()
	traverseDirectories()
}
