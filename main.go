// firehose project main.go
package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	queueDir     = "/tmp/"
	scanInterval time.Duration
	ch           = make(chan string)
)

func traverseDirectories(ch chan string) {

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
			go checkDirectory(ch, tweetdirectory)
		}
		time.Sleep(scanInterval)
		fmt.Print(time.Now(), " Success\n")
	}
}

func checkDirectory(ch chan string, tweetdirectory os.FileInfo) {
	if tweetdirectory.IsDir() && strings.Contains(tweetdirectory.Name(), "tweet") {
		//fmt.Print("found dir: ", tweetdirectory.Name(), "\n")
		sparkfiles, _ := ioutil.ReadDir(filepath.Join(queueDir, tweetdirectory.Name()))
		for _, sparkfile := range sparkfiles {
			if tweetdirectory.IsDir() && strings.Contains(sparkfile.Name(), "part") && !strings.HasPrefix(sparkfile.Name(), ".") {
				//fmt.Print("found file: ", sparkfile.Name(), "\n")
				content, err := ioutil.ReadFile(filepath.Join(queueDir, tweetdirectory.Name(), sparkfile.Name()))
				if err != nil {
					fmt.Print(err)
					os.Exit(1)
				}
				lines := strings.Split(string(content), "\n")
				for _, sqlstmt := range lines {
					if strings.Contains(sqlstmt, "INSERT") {
						//fmt.Print("found stmt: ", sqlstmt, "\n")
						ch <- sqlstmt
					}
				}
			}
		}

		fmt.Print("Action=RemoveDir, Directory=", tweetdirectory.Name())
		err := os.RemoveAll(filepath.Join(queueDir, tweetdirectory.Name()))
		if err != nil {
			fmt.Print("status=Remove Directory Failed, error=", err)
		}
	}
}

func processStmts(ch chan string) {
	db, err := sql.Open("mysql", "root:everythingisawesome@tcp(localhost:3306)/seeitremix")
	if err != nil {
		fmt.Print("connection error: ")
		fmt.Print(err)
		os.Exit(1)
	}

	defer db.Close()

	var input string

	for {
		input = <-ch
		//fmt.Print("attempting: ", input)
		_, err := db.Exec(input)
		if err != nil {
			fmt.Print("statement error=", err, "\n")
			fmt.Print("sqlstmt=", input, "\n")
		}
	}
}

func main() {
	flag.DurationVar(&scanInterval, "i", time.Duration(5*time.Second), "scan interval")
	flag.Parse()
	go processStmts(ch)
	traverseDirectories(ch)
}
