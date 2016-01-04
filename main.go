package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/robfig/config"
)

type Paste struct {
	ScrapeURL string `json:"scrape_url"`
	FullURL   string `json:"full_url"`
	Date      string `json:"date"`
	Key       string `json:"key"`
	Size      string `json:"size"`
	Expire    string `json:"expire"`
	Title     string `json:"title"`
	Syntax    string `json:"syntax"`
	User      string `json:"user"`
}

var conf *config.Config
var configFile = flag.String("c", "pastebin_scrape.conf", "specify config file")
var dsn string
var pasteLimit int
var stmt = map[string]*sql.Stmt{}

func main() {
	flag.Parse()

	conf, err := config.ReadDefault(*configFile)
	if err != nil {
		conf = config.NewDefault()
		fmt.Printf("Error loading config file")
	}

	dsn, err = conf.String("DEFAULT", "DSN")
	if err != nil {
		log.Printf("Error reading DSN: %s", err)
	}

	runTest, _ := conf.Bool("DEFAULT", "TestMode")
	monitorInterval, _ := conf.Int("DEFAULT", "MonitorInterval")
	monInt := time.Duration(monitorInterval)
	pasteLimit, _ = conf.Int("DEFAULT", "pasteLimit")

	log.SetFlags(log.Ldate | log.Ltime)
	logDir, _ := conf.String("DEFAULT", "log_dir")
	if logDir == "" {
		logDir = "."
	}

	filePrefix := "pastebin_scrape-"
	fnTime := time.Now().UTC().Format("200601")

	logFile := fmt.Sprintf("%s/%s%s.log", logDir, filePrefix, fnTime)
	fp, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_SYNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file '%s': %s", logFile, err)
	}

	log.SetOutput(fp)

	initDB(dsn)
	initTables()

	log.Printf("Starting monitoring with an interval of '%d' minutes with a limit of '%d' pastes per request.", monitorInterval, pasteLimit)

	if runTest == true {
		test()
	} else {
		run(monInt)
	}

}

func run(i time.Duration) {
	tick := time.NewTicker(time.Minute * i).C

	for {
		select {
		case <-tick:
			beg := time.Now()
			getListing()
			getContents()
			log.Printf("Completed scrape in %.2f seconds.", time.Since(beg).Seconds())
		}
	}
}

func test() {
	getListing()
	getContents()
}

func initTables() {

	_, err := stmt["createPastebinIndex"].Exec()
	if err != nil {
		log.Printf("Error creating pastebin_index table: %s", err)
	}

	_, err = stmt["createPastebinContent"].Exec()
	if err != nil {
		log.Printf("Error creating pastebin_content table: %s", err)
	}

}

func getListing() {
	log.Println("Getting Listing...")
	req, err := http.NewRequest("GET", fmt.Sprintf("http://pastebin.com/api_scraping.php?limit=%d", pasteLimit), nil)
	req.Header.Set("Accept", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Printf("Error getting listing from pastebin: %s", err)
	}

	if res.StatusCode != 200 {
		log.Printf("There was a non-200 response given from pastebin: %d", res.StatusCode)
		res.Body.Close()
		return
	}

	decoded := make([]Paste, 0)
	body, err := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(body, &decoded)

	res.Body.Close()

	if err != nil {
		log.Printf("Decode error: %s", err)
		return
	}

	for _, v := range decoded {
		_, err = stmt["insertIndex"].Exec(v.Key, v.ScrapeURL, v.FullURL, v.Date, v.Size, v.Expire, v.Expire, v.Title, v.Syntax, v.User)
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Error 1062: Duplicate entry") {
			log.Printf("Error inserting data into pastebin_index: %s", err)
		}
	}
}

func getContents() {
	log.Println("Getting contents and storing...")

	rows, err := stmt["getKeys"].Query()

	if err != nil {
		log.Printf("Query error: %s", err)
	}
	var pasteKey string
	keysToProc := map[string]string{}
	for rows.Next() {
		err = rows.Scan(&pasteKey)
		if err != nil {
			log.Printf("Scan error: %s", err)
		}
		keysToProc[pasteKey] = pasteKey
	}
	rows.Close()

	for k, _ := range keysToProc {
		time.Sleep(1) //This is to satisfy pastebin so they dont get mad.
		req, err := http.NewRequest("GET", fmt.Sprintf("http://pastebin.com/api_scrape_item.php?i=%s", k), nil)
		req.Header.Set("Accept", "application/json")
		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			log.Printf("Error getting listing from pastebin: %s", err)
		}

		if res.StatusCode != 200 {
			log.Printf("There was a non-200 response given from pastebin: %d", res.StatusCode)
			res.Body.Close()
			return
		}

		b, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		sum := sha256.Sum256([]byte(b))
		sumString := fmt.Sprintf("%x", sum[:])

		_, err = stmt["insertContent"].Exec(k, b, sumString)
		if err != nil {
			log.Printf("Error inserting content for %s: %s", k, err)
		}
	}

	log.Println("Done.")
}
