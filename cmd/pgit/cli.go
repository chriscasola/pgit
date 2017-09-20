package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/chriscasola/pgit"
)

func main() {
	dbURL := flag.String("database", "", "PSQL url of the database")
	rootPath := flag.String("root", "", "path to the root of the schema definition files")

	flag.Parse()

	printUsage := func() {
		fmt.Println("Usage: pgit [options] command\ncommand is one of migrate or rollback")
		flag.PrintDefaults()
	}

	if flag.NFlag() != 2 || len(flag.Args()) != 1 {
		printUsage()
		os.Exit(1)
	}

	command := flag.Arg(0)

	conn, err := pgit.NewSQLDatabaseConnection(*dbURL, "")

	if err != nil {
		fmt.Printf("Error connecting to DB: %v\n", err)
		os.Exit(1)
	}

	instance, err := pgit.New(*rootPath, conn)

	if err != nil {
		fmt.Printf("Error initializing Pgit: %v\n", err)
		os.Exit(1)
	}

	if command == "migrate" {
		if err = instance.ApplyLatest(); err != nil {
			fmt.Printf("Error updating the database to the latest schema: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("Finished applying latest version of schemas to the database.")
		os.Exit(0)
	}

	if command == "rollback" {
		if err = instance.Rollback(); err != nil {
			fmt.Printf("Error rolling back last migration: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("Rolled back last migration")
		os.Exit(0)
	}
}
