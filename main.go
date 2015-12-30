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

	_ "github.com/go-sql-driver/mysql"
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

type Pastes struct {
	Paste []Paste
}

var conf *config.Config
var configFile = flag.String("c", "pastebin_scrape.conf", "specify config file")
var dsn string
var pasteLimit int

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

	initDB()

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

func initDB() {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Database error: %s", err)
	}

	sql := `CREATE TABLE IF NOT EXISTS pastebin_index
    (
		pastebin_key VARCHAR(50) NOT NULL PRIMARY KEY,
		scrape_url varchar(255),
        full_url varchar(255),
		date DATETIME,
		size BIGINT,
        expire DATETIME,
        title varchar(255),
        syntax varchar(255),
        user varchar(255),
		enterdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`

	_, err = db.Exec(sql)
	if err != nil {
		log.Printf("Error creating pastebin_index table: %s", err)
	}

	sql = `CREATE TABLE IF NOT EXISTS pastebin_content
    (
        pastebin_key varchar(50) NOT NULL PRIMARY KEY,
        content TEXT,
        hash varchar(255),
        enterdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`

	_, err = db.Exec(sql)
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
		storeIndex(v)
	}
}

func storeIndex(m Paste) {
	log.Println("Storing listings...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Database error: %s", err)
	}

	sql := `INSERT INTO pastebin_index (pastebin_key, scrape_url, full_url, date, size, expire, title, syntax, user) VALUES (?,?,?, FROM_UNIXTIME(?), ?,IF(? > 0, FROM_UNIXTIME(?), NULL),?,?,?);`
	stmt, _ := db.Prepare(sql)
	_, err = stmt.Exec(m.Key, m.ScrapeURL, m.FullURL, m.Date, m.Size, m.Expire, m.Expire, m.Title, m.Syntax, m.User)
	if err != nil && !strings.Contains(fmt.Sprintf("%s", err), "Error 1062: Duplicate entry") {
		log.Printf("Error inserting data into pastebin_index: %s", err)
	}
}

func getContents() {
	log.Println("Getting contents and storing...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Database error: %s", err)
	}

	sql := `SELECT pastebin_index.pastebin_key
    FROM pastebin_index
    LEFT JOIN pastebin_content ON pastebin_content.pastebin_key = pastebin_index.pastebin_key
    WHERE pastebin_content.pastebin_key IS NULL
    AND IFNULL(pastebin_index.expire,NOW()) >= NOW();`

	rows, err := db.Query(sql)

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

		inSql := `INSERT INTO pastebin_content (pastebin_key, content, hash) VALUES (?,?,?)`
		stmt, _ := db.Prepare(inSql)
		_, err = stmt.Exec(k, b, sumString)
		if err != nil {
			log.Printf("Error inserting content for %s: %s", k, err)
		}
	}
	log.Println("Done.")
}
