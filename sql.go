package main

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func initDB(dsn string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %s", err)
	}

	initStmt(db, "createPastebinIndex", `CREATE TABLE IF NOT EXISTS pastebin_index
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
	);`)
	initStmt(db, "createPastebinContent", `CREATE TABLE IF NOT EXISTS pastebin_content
    (
        pastebin_key varchar(50) NOT NULL PRIMARY KEY,
        content TEXT,
        hash varchar(255),
        enterdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`)

	initStmt(db, "insertIndex", `INSERT INTO pastebin_index (pastebin_key, scrape_url, full_url, date, size, expire, title, syntax, user) VALUES (?,?,?, FROM_UNIXTIME(?), ?,IF(? > 0, FROM_UNIXTIME(?), NULL),?,?,?);`)
	initStmt(db, "getKeys", `SELECT pastebin_index.pastebin_key
    FROM pastebin_index
    LEFT JOIN pastebin_content ON pastebin_content.pastebin_key = pastebin_index.pastebin_key
    WHERE pastebin_content.pastebin_key IS NULL
    AND IFNULL(pastebin_index.expire,NOW()) >= NOW();`)
	initStmt(db, "insertContent", `INSERT INTO pastebin_content (pastebin_key, content, hash) VALUES (?,?,?)`)

}

func initStmt(db *sql.DB, name, query string) {
	var err error

	stmt[name], err = db.Prepare(query)
	if err != nil {
		log.Fatalf("Error preparing %s: %s\n\t%s\n", name, err, query)
	}
}
