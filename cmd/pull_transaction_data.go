package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic(err)
	}

	gdriveURL := fmt.Sprintf(
		"https://docs.google.com/uc?export=download&id=%s",
		os.Getenv("GDRIVE_CSV_ID"))

	outFileName := fmt.Sprintf(
		"data/%s.csv",
		time.Now().Format("2006.01.02"))

	out, err := os.Create(outFileName)
	if err != nil {
		panic(err)
	}

	resp, err := http.Get(gdriveURL)
	defer resp.Body.Close()
	if err != nil {
		panic(err)
	}

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(fmt.Sprintf("Wrote %d bytes to %s", n, outFileName))
}
