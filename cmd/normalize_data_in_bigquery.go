package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
	"os"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic(err)
	}

	credsPath := os.Getenv("CREDS_PATH")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"),
		option.WithCredentialsFile(credsPath))
	if err != nil {
		panic(err)
	}

	querySQL := fmt.Sprintf(`
INSERT INTO %s
(day, project_id, volume, total_transactions)
SELECT
  DATE(timestamp) AS day,
  project_id,
  SUM(value) AS volume,
  COUNT(*) AS total_transactions
FROM %s
GROUP BY DATE(timestamp), project_id
ORDER BY day, project_id;`, "`sequenceinterview.transactions.tx_flat`", "`sequenceinterview.transactions.tx`")

	query := client.Query(querySQL)
	query.Location = "US"

	job, err := query.Run(ctx)
	if err != nil {
		panic(err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		panic(err)
	}

	if err := status.Err(); err != nil {
		panic(err)
	}

	fmt.Println("Done!")

}
