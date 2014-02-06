package main

import (
	"flag"
	"fmt"
	"github.com/stretchr/pushcsv/io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

/*

  pushcsv
  - Command line tool for pushing CSV files of any size into Stretchr

  To build:  go build -o pushcsv

  ---

  pushcsv reads, a line at a time, a CSV file of any size and POSTs the
  data to a Stretchr end-point.

*/

var (
	url   = flag.String("to", "", "The URL to send the CSV data to.")
	lines = flag.Int("lines", 100, "The number of lines to send in each request.")
)

func main() {

	flag.Parse()
	reader := io.NewLineReader(os.Stdin)

	// read the first line - the header line
	var headersLine string
	if headersLineBytes, err := reader.ReadLine(); err == nil {
		headersLine = string(headersLineBytes)
	} else {
		fatal("Failed to read header line: %s", err)
	}

	var readErr error = nil
	linesBuffer := make([]string, *lines)
	var lineBytes []byte
	var intoBuffer int = 0
	var totalPushed int = 0

	for readErr == nil {
		for readErr == nil && intoBuffer < *lines {
			if lineBytes, readErr = reader.ReadLine(); readErr == nil {
				linesBuffer[intoBuffer] = string(lineBytes)
				//log("- %d) %s", intoBuffer, linesBuffer[intoBuffer])
				intoBuffer++
			}
		}

		if intoBuffer > 0 {

			// push these into Stretchr
			totalPushed += intoBuffer

			lineData := []string{headersLine}
			lineData = append(lineData, linesBuffer[:intoBuffer]...)
			payload := strings.Join(lineData, "\n")

			log("POST %s", *url)
			log("%s", payload)
			log("...")

			req, err := http.NewRequest("POST", *url, strings.NewReader(payload))
			if err != nil {
				fatal("Request is invalid: ", err)
			}
			req.Header.Set("Content-Type", "text/csv")

			log("Making request: %s", req)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				fatal("Unable to make request: ", err)
			}
			defer res.Body.Close()

			resBod, _ := ioutil.ReadAll(res.Body)
			log("Status: %d", res.StatusCode)
			log("%s\n", string(resBod))
			log("")
			log("")

			if res.StatusCode > 299 {
				log("Cancelling... something went wrong.")
				break
			}

			// reset
			intoBuffer = 0

		}

	}

	log("Finished.")

}

func log(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func fatal(format string, args ...interface{}) {
	log(format, args...)
	os.Exit(1)
}
