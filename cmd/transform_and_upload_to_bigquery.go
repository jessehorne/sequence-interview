package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"time"
)

// CSVRow represents a single row from the CSV data source we're pulling from containing
// only the information we want in the appropriate format
type CSVRow struct {
	Timestamp string `bigquery:"timestamp"`
	Event     string `bigquery:"event"`
	ProjectID int    `bigquery:"project_id"`
	Value     int    `bigquery:"value"` // value represents the value in USD where 1 = 1 penny
}

type Props struct {
	CurrencySymbol string `json:"currencySymbol"`
}

type Nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}

const (
	TIME_FORMAT_CSV = "2006-01-02 15:04:05"
	TIME_FORMAT_BQ  = "2006-01-02T15:04:05"
)

// getCoinValueInUSD gets a random value or from the cache for a given currency symbol
// I decided to not use coin gecko for this challenge due to time constraints and rate-limits I kept hitting
// This method returns the value as an int where 1 = 1 penny. So, 1000 would be 10 USD.
func getCoinValueInUSD(cache map[string]int, id string) int {
	_, ok := cache[id]
	if !ok {
		// generate value and fill in map
		cache[id] = int((1.0 - math.Cos(rand.Float64())) * 10000) // random number favoring smaller numbers
	}

	return cache[id]
}

func main() {
	if err := godotenv.Load(); err != nil {
		panic(err)
	}

	if len(os.Args) == 1 || len(os.Args) > 2 {
		fmt.Println("Invalid number of options. Try `go run cmd/transform_and_upload_to_bigquery.go <FILEPATH>")
		return
	}

	filepath := os.Args[1]

	// read csv line by line
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}

	// set up currency cache for storing USD values of currencies at a given time (time part is TODO)
	currencyValueCache := map[string]int{}

	rows := make([]CSVRow, 0)
	csvReader := csv.NewReader(file)
	firstLine := true
	for {
		values, err := csvReader.Read()
		if err != nil {
			break
		}

		if err == io.EOF {
			break
		}

		if firstLine {
			firstLine = false
			continue
		}

		// we now need to process the columns
		created, err := time.Parse(TIME_FORMAT_CSV, values[1])
		if err != nil {
			fmt.Println(err)
			continue
		}

		projectID, err := strconv.Atoi(values[3])
		if err != nil {
			fmt.Println(err)
			continue
		}

		// get prop currency symbol
		propJSON := values[14]
		var props Props
		if err := json.Unmarshal([]byte(propJSON), &props); err != nil {
			fmt.Println(err)
			continue
		}

		// get nums currency value decimal
		numsJSON := values[15]
		var nums Nums
		if err := json.Unmarshal([]byte(numsJSON), &nums); err != nil {
			fmt.Println(err)
			continue
		}

		newRow := CSVRow{
			Timestamp: created.Format(TIME_FORMAT_BQ),
			Event:     values[2],
			ProjectID: projectID,
			Value:     getCoinValueInUSD(currencyValueCache, props.CurrencySymbol),
		}

		rows = append(rows, newRow)
	}

	// upload to big query
	credsPath := os.Getenv("CREDS_PATH")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"),
		option.WithCredentialsFile(credsPath))
	if err != nil {
		panic(err)
	}

	dataset := client.Dataset(os.Getenv("GCP_BQ_DATASET"))
	table := dataset.Table("tx")
	inserter := table.Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		panic(err)
	}

	fmt.Println("Uploaded transactions")
}
