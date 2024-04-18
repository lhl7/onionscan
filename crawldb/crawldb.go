package crawldb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"onionscanv3/model"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// CrawlDB is the main interface for persistent storage in OnionScan
type CrawlDB struct {
	myDB *sql.DB
}

// NewDB creates a new CrawlDB instance. If the database does not exist at the
// given dbURL, it will be created.
func (cdb *CrawlDB) NewDB(dbURL string) {
	db, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatal(err)
	}
	cdb.myDB = db

	// If we have just created this db, then it will be empty
	if err := cdb.initialize(); err != nil {
		log.Fatal(err)
	}
}

// initialize sets up a new database - should only be called when creating a
// new database.
// There is a lot of indexing here, which may seem overkill - but on a large
// OnionScan run these indexes take up < 100MB each - which is really cheap
// when compared with their search potential.
func (cdb *CrawlDB) initialize() error {
	// Creating Database Bucket crawls
	_, err := cdb.myDB.Exec(`
		CREATE TABLE IF NOT EXISTS crawls (
			ID INT AUTO_INCREMENT PRIMARY KEY,
			URL VARCHAR(255),
			Timestamp TIMESTAMP,
			Page TEXT
		)
	`)
	if err != nil {
		return err
	}

	// Allow searching by the URL
	_, err = cdb.myDB.Exec(`
		CREATE INDEX IF NOT EXISTS crawls_url_index ON crawls (URL)
	`)
	if err != nil {
		return err
	}

	// Creating Database Bucket relationships
	_, err = cdb.myDB.Exec(`
		CREATE TABLE IF NOT EXISTS relationships (
			ID INT AUTO_INCREMENT PRIMARY KEY,
			Onion VARCHAR(255),
			FromValue VARCHAR(255),
			Type VARCHAR(255),
			Identifier VARCHAR(255),
			FirstSeen TIMESTAMP,
			LastSeen TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	// Allowing searching by the Identifier String
	_, err = cdb.myDB.Exec(`
		CREATE INDEX IF NOT EXISTS relationships_identifier_index ON relationships (Identifier)
	`)
	if err != nil {
		return err
	}

	// Allowing searching by the Onion String
	_, err = cdb.myDB.Exec(`
		CREATE INDEX IF NOT EXISTS relationships_onion_index ON relationships (Onion)
	`)
	if err != nil {
		return err
	}

	// Allowing searching by the Type String
	_, err = cdb.myDB.Exec(`
		CREATE INDEX IF NOT EXISTS relationships_type_index ON relationships (Type)
	`)
	if err != nil {
		return err
	}

	// Allowing searching by the From String
	_, err = cdb.myDB.Exec(`
		CREATE INDEX IF NOT EXISTS relationships_from_index ON relationships (FromValue)
	`)
	if err != nil {
		return err
	}

	log.Printf("Database Setup Complete")
	return nil
}

// CrawlRecord defines a spider entry in the database
type CrawlRecord struct {
	ID        int
	URL       string
	Timestamp time.Time
	Page      model.Page
}

// InsertCrawlRecord adds a new spider entry to the database and returns the
// record id.
func (cdb *CrawlDB) InsertCrawlRecord(url string, page *model.Page) (int, error) {
	// Serialize the Page to JSON
	pageJSON, err := json.Marshal(page)
	if err != nil {
		return 0, err
	}

	stmt, err := cdb.myDB.Prepare(`
		INSERT INTO crawls (URL, Timestamp, Page)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(url, time.Now(), pageJSON)
	if err != nil {
		return 0, err
	}

	docID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(docID), nil
}

// GetCrawlRecord returns a CrawlRecord from the database given an ID.
func (cdb *CrawlDB) GetCrawlRecord(id int) (CrawlRecord, error) {
	row := cdb.myDB.QueryRow(`
		SELECT ID, URL, Timestamp, Page
		FROM crawls
		WHERE ID = ?
	`, id)

	var crawlRecord CrawlRecord
	var pageJSON string // Store serialized Page as a string
	var timestampStr string

	err := row.Scan(&crawlRecord.ID, &crawlRecord.URL, timestampStr, &pageJSON)
	if err != nil {
		return CrawlRecord{}, err
	}
	crawlRecord.Timestamp, err = time.Parse("2006-01-02 15:04:05", timestampStr)

	// Deserialize the Page from JSON
	err = json.Unmarshal([]byte(pageJSON), &crawlRecord.Page)
	if err != nil {
		return CrawlRecord{}, err
	}

	return crawlRecord, nil
}

// HasCrawlRecord returns true if a given URL is associated with a crawl record
// in the database. Only records created after the given duration are considered.
func (cdb *CrawlDB) HasCrawlRecord(url string, duration time.Duration) (bool, int) {
	before := time.Now().Add(duration)

	row := cdb.myDB.QueryRow(`
        SELECT ID, Timestamp
        FROM crawls
        WHERE URL = ? AND Timestamp > ?
    `, url, before)

	var id int
	var timestampStr string // Change the type to string
	err := row.Scan(&id, &timestampStr)
	if err == sql.ErrNoRows {
		return false, 0
	} else if err != nil {
		panic(err)
	}

	// Convert the timestamp string to time.Time
	//timestamp, err := time.Parse("2006-01-02 15:04:05", timestampStr)
	if err != nil {
		panic(err)
	}

	return true, id
}

// Relationship defines a correlation record in the Database.
type Relationship struct {
	ID         int
	Onion      string
	From       string
	Type       string
	Identifier string
	FirstSeen  time.Time
	LastSeen   time.Time
}

// InsertRelationship creates a new Relationship in the database.
// InsertRelationship creates a new Relationship in the database.
func (cdb *CrawlDB) InsertRelationship(onion string, from string, identifierType string, identifier string) (int, error) {
	// Check if the relationship already exists
	rels, err := cdb.GetRelationshipsWithOnion(onion)

	// If we have seen this before, we will update rather than adding a new relationship
	if err == nil {
		for _, rel := range rels {
			if rel.From == from && rel.Identifier == identifier && rel.Type == identifierType {
				// Update the Relationship
				log.Printf("Updating %s --- %s ---> %s (%s)", onion, from, identifier, identifierType)
				return cdb.UpdateRelationship(rel.ID, onion, from, identifierType, identifier)
			}
		}
	}

	// Otherwise, insert a new relationship
	log.Printf("Inserting %s --- %s ---> %s (%s)", onion, from, identifier, identifierType)
	return cdb.InsertNewRelationship(onion, from, identifierType, identifier)
}

// InsertNewRelationship inserts a new Relationship in the database.
func (cdb *CrawlDB) InsertNewRelationship(onion string, from string, identifierType string, identifier string) (int, error) {
	result, err := cdb.myDB.Exec(`
		INSERT INTO relationships (Onion, FromValue, Type, Identifier, FirstSeen, LastSeen)
		VALUES (?, ?, ?, ?, ?, ?)
	`, onion, from, identifierType, identifier, time.Now(), time.Now())

	if err != nil {
		return 0, err
	}

	lastInsertID, _ := result.LastInsertId()
	return int(lastInsertID), nil
}

// UpdateRelationship updates an existing Relationship in the database.
func (cdb *CrawlDB) UpdateRelationship(id int, onion string, from string, identifierType string, identifier string) (int, error) {
	result, err := cdb.myDB.Exec(`
		UPDATE relationships
		SET Onion=?, FromValue=?, Type=?, Identifier=?, LastSeen=?
		WHERE ID=?
	`, onion, from, identifierType, identifier, time.Now(), id)

	if err != nil {
		return 0, err
	}

	rowsAffected, _ := result.RowsAffected()
	return int(rowsAffected), nil
}

// GetRelationshipsWithOnion returns all relationships with an Onion field matching
// the onion parameter.
func (cdb *CrawlDB) GetRelationshipsWithOnion(onion string) ([]Relationship, error) {
	return cdb.queryDB("Onion", onion)
}

func (cdb *CrawlDB) queryDB(field string, value string) ([]Relationship, error) {
	fmt.Println(value)
	rows, err := cdb.myDB.Query(`
        SELECT ID, Onion, FromValue, Type, Identifier, FirstSeen, LastSeen
        FROM relationships
        WHERE Onion = ?
    `, value)
	if err != nil {
		return nil, err
	}
	//fmt.Println(rows)
	defer rows.Close()

	var rels []Relationship
	for rows.Next() {
		var relationship Relationship
		var firstSeenBytes, lastSeenBytes []byte // Change the types to []byte
		err := rows.Scan(&relationship.ID, &relationship.Onion, &relationship.From, &relationship.Type, &relationship.Identifier, &firstSeenBytes, &lastSeenBytes)
		fmt.Println("sahdasidk")
		if err != nil {
			return nil, err
		}

		// Convert the timestamp bytes to string and then to time.Time
		if len(firstSeenBytes) > 0 {
			relationship.FirstSeen, err = time.Parse("2006-01-02 15:04:05", string(firstSeenBytes))
			if err != nil {
				return nil, err
			}
		}

		if len(lastSeenBytes) > 0 {
			relationship.LastSeen, err = time.Parse("2006-01-02 15:04:05", string(lastSeenBytes))
			if err != nil {
				return nil, err
			}
		}

		rels = append(rels, relationship)
	}
	fmt.Println(rels)
	return rels, nil
}

// GetUserRelationshipFromOnion reconstructs a user relationship from a given
// identifier. fromonion is used as a filter to ensure that only user relationships
// from a given onion are reconstructed.
func (cdb *CrawlDB) GetUserRelationshipFromOnion(identifier string, fromonion string) (map[string]Relationship, error) {
	relationships := make(map[string]Relationship)

	rows, err := cdb.myDB.Query(`
		SELECT ID, Onion, FromValue, Type, Identifier, FirstSeen, LastSeen
		FROM relationships
		WHERE Onion = ? AND FromValue = ?
	`, identifier, fromonion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var relationship Relationship
		err := rows.Scan(&relationship.ID, &relationship.Onion, &relationship.From, &relationship.Type, &relationship.Identifier, &relationship.FirstSeen, &relationship.LastSeen)
		if err != nil {
			return nil, err
		}

		relationships[relationship.Type] = relationship
	}

	return relationships, nil
}

// GetAllRelationshipsCount returns the total number of relationships stored in
// the database.
func (cdb *CrawlDB) GetAllRelationshipsCount() int {
	var count int

	row := cdb.myDB.QueryRow(`
		SELECT COUNT(*)
		FROM relationships
	`)

	err := row.Scan(&count)
	if err != nil {
		return 0
	}

	return count
}

// GetRelationshipsCount returns the total number of relationships for a given
// identifier.
func (cdb *CrawlDB) GetRelationshipsCount(identifier string) int {
	var count int

	row := cdb.myDB.QueryRow(`
		SELECT COUNT(*)
		FROM relationships
		WHERE Identifier = ?
	`, identifier)

	err := row.Scan(&count)
	if err != nil {
		return 0
	}

	return count
}

// GetRelationshipsWithIdentifier returns all relationships associated with a
// given identifier.
func (cdb *CrawlDB) GetRelationshipsWithIdentifier(identifier string) ([]Relationship, error) {
	var queryResult []Relationship

	rows, err := cdb.myDB.Query(`
		SELECT ID, Onion, FromValue, Type, Identifier, FirstSeen, LastSeen
		FROM relationships
		WHERE Type = ? OR FromValue = ? OR Identifier = ?
	`, identifier, identifier, identifier)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var relationship Relationship
		var firstSeenBytes, lastSeenBytes []byte // Change the types to []byte
		err := rows.Scan(&relationship.ID, &relationship.Onion, &relationship.From, &relationship.Type, &relationship.Identifier, &firstSeenBytes, &lastSeenBytes)
		if err != nil {
			return nil, err
		}

		// Convert the timestamp bytes to string and then to time.Time
		if len(firstSeenBytes) > 0 {
			relationship.FirstSeen, err = time.Parse("2006-01-02 15:04:05", string(firstSeenBytes))
			if err != nil {
				return nil, err
			}
		}

		if len(lastSeenBytes) > 0 {
			relationship.LastSeen, err = time.Parse("2006-01-02 15:04:05", string(lastSeenBytes))
			if err != nil {
				return nil, err
			}
		}

		queryResult = append(queryResult, relationship)
	}

	return queryResult, nil
}

// DeleteRelationship deletes a relationship given the quad.
func (cdb *CrawlDB) DeleteRelationship(onion string, from string, identifierType string, identifier string) error {
	stmt, err := cdb.myDB.Prepare(`
		DELETE FROM relationships
		WHERE Onion = ? AND FromValue = ? AND Type = ? AND Identifier = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(onion, from, identifierType, identifier)
	if err != nil {
		return err
	}

	return nil
}
