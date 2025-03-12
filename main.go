package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

type MSDSCourse struct {
	CID     string `json:"courseI_D"`
	CNAME   string `json:"course_name"`
	CPREREQ string `json:"prerequisite"`
}

var db *sql.DB

func init() {
	var err error

	fmt.Println("Initializing the DB connection")

	db_connection := "user=postgres dbname=msds_course_catalog password=sql host=/cloudsql/msds432assignment6-452413:us-central1:mypostgres sslmode=disable port=5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal(fmt.Println("Couldn't Open Connection to database"))
		panic(err)
	}
}

func main() {
	log.Print("starting CBI Microservices ...")

	createTable(db)
	listHandler(db)
	insertHandler(db)
	deleteHandler(db)
	searchHandler(db)

	http.HandleFunc("/", handler)

	mux := http.NewServeMux()
	mux.Handle("/list", listHandler(db))
	mux.Handle("/insert", insertHandler(db))
	mux.Handle("/delete/", deleteHandler(db))
	mux.Handle("/search/", searchHandler(db))

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	log.Print("Navigate to Cloud Run services and find the URL of your service")
	log.Print("Use the browser and navigate to your service URL to to check your service has started")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}

}

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "MSDS-Course-Catalog" //maybe need to
	}

	fmt.Fprintf(w, "microservices' goroutines have started for %s!\n", name)
}

func createTable(db *sql.DB) {
	drop_table := `drop table if exists courses`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "courses" (
		"course_id"   VARCHAR(10) ,
		"course_name" VARCHAR(255) ,
		"prerequisite" VARCHAR(255) ,
		PRIMARY KEY ("course_id")
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for msds course catalog")

	sql := `INSERT INTO courses (course_id, course_name, prerequisite)
	VALUES 
    ('MSDS400',	'Math for Modelers', 'None'),
    ('MSDS485','Data Governance, Ethics, and Law','None'),
    ('MSDS403','Data Science and Digital Transformation','None'),
    ('MSDS460','Decision Analytics','MSDS400, MSDS4001'),
    ('MSDS432','Foundations Of Data Engineering','MSDS420');`

	_, err = db.Exec(
		sql)

	if err != nil {
		panic(err)
	}

	fmt.Println("Completed Inserting Rows into the courses Table")
}

// Fetch all courses from the database
func listCourses(db *sql.DB) ([]MSDSCourse, error) {
	rows, err := db.Query("SELECT course_id, course_name, prerequisite FROM courses")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var courses []MSDSCourse
	for rows.Next() {
		var course MSDSCourse
		if err := rows.Scan(&course.CID, &course.CNAME, &course.CPREREQ); err != nil {
			return nil, err
		}
		courses = append(courses, course)
	}
	return courses, nil
}

// Insert a new course into the database
func insertCourse(db *sql.DB, course MSDSCourse) error {
	_, err := db.Exec("INSERT INTO courses (course_id, course_name, prerequisite) VALUES ($1, $2, $3)",
		course.CID, course.CNAME, course.CPREREQ)
	return err
}

// Delete a course from the database
func deleteCourse(db *sql.DB, courseID string) error {
	_, err := db.Exec("DELETE FROM courses WHERE course_id = $1", courseID)
	return err
}

// Search for a course by ID
func searchCourse(db *sql.DB, courseID string) (*MSDSCourse, error) {
	var course MSDSCourse
	err := db.QueryRow("SELECT course_id, course_name, prerequisite FROM courses WHERE course_id = $1", courseID).
		Scan(&course.CID, &course.CNAME, &course.CPREREQ)
	if err != nil {
		return nil, err
	}
	return &course, nil
}

// Handlers for HTTP routes
func listHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		courses, err := listCourses(db)
		if err != nil {
			http.Error(w, "Failed to retrieve courses", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(courses)
	}
}

func insertHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var course MSDSCourse
		if err := json.NewDecoder(r.Body).Decode(&course); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		err := insertCourse(db, course)
		if err != nil {
			http.Error(w, "Insert failed", http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Course inserted"))
	}
}

func deleteHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		courseID := r.URL.Path[len("/delete/"):]
		if err := deleteCourse(db, courseID); err != nil {
			http.Error(w, "Delete failed", http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Course deleted"))
	}
}

func searchHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		courseID := r.URL.Path[len("/search/"):]
		course, err := searchCourse(db, courseID)
		if err != nil {
			http.Error(w, "Course not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(course)
	}
}
