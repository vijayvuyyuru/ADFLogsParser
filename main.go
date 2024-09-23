package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/edaniels/golog"
	"github.com/pkg/errors"
	"go.viam.com/utils"
)

var (
	logger    = golog.NewDebugLogger("log_parsing")
	listFiles = false
)

type arguments struct {
	LogPath    string `flag:"log_path,usage=file path of log file can be zipped or unzipped,required=true"`
	OutputPath string `flag:"output_path,usage=output text file,required=true"`
	ListFiles  bool   `flag:"list_files,usage=whether to include a full list of files parsed in output,default=false"`
}

type queryData struct {
	query         map[string]any
	timeStamp     string
	correlationID string
	files         []string
}

func (q queryData) String() string {
	var sb strings.Builder
	bytes, _ := json.MarshalIndent(q.query, "", "    ")

	sb.WriteString("query:")
	sb.Write(bytes)
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("time: %s\n", q.timeStamp))
	sb.WriteString(fmt.Sprintf("correlationID: %s\n", q.correlationID))
	sb.WriteString(fmt.Sprintf("read %d files\n", len(q.files)))
	if listFiles {
		sb.WriteString("files:\n")
		for _, file := range q.files {
			sb.WriteString(fmt.Sprintf("\t%s\n", file))
		}
	}
	sb.WriteString("\n\n")
	return sb.String()
}

func exportToFile(queries []queryData, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, query := range queries {
		if len(query.files) != 0 {
			_, err := f.WriteString(query.String())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	utils.ContextualMain(mainWithArgs, logger)
}

func mainWithArgs(ctx context.Context, args []string, logger golog.Logger) error {
	var argsParsed arguments
	if err := utils.ParseFlags(args, &argsParsed); err != nil {
		return err
	}
	listFiles = argsParsed.ListFiles

	file, err := os.Open(argsParsed.LogPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.Contains(argsParsed.LogPath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		reader = gzReader
	}

	scanner := bufio.NewScanner(reader)
	//make the buffer long to accomodate long strings
	buf := make([]byte, 1_000_000)
	scanner.Buffer(buf, 1_000_000)

	queryCount := 0
	var activeQuery queryData
	queries := []queryData{}
	for scanner.Scan() {
		text := scanner.Text()
		var decodedData map[string]interface{}
		err = json.Unmarshal(scanner.Bytes(), &decodedData)

		idRaw, ok := decodedData["correlationID"]
		if !ok {
			continue
		}
		id := idRaw.(string)

		if strings.Contains(text, "executing query plan") {

			// when query found, reset active query and save the current one
			queries = append(queries, activeQuery)
			queryCount++
			if err != nil {
				return err
			}
			query, ok := decodedData["plan"]
			if !ok {
				return errors.New("failed to find query")
			}

			activeQuery = queryData{
				query:         query.(map[string]any),
				timeStamp:     decodedData["internalTimestamp"].(string),
				correlationID: id,
			}
		} else if strings.Contains(text, "open partition") {
			activeQuery.files = append(activeQuery.files, decodedData["source"].(string))
		}

	}

	err = exportToFile(queries, argsParsed.OutputPath)
	if err != nil {
		return err
	}
	if len(queries) == 1 && queries[0].correlationID == "" {
		logger.Warn("No query found in logs")
	}
	return scanner.Err()
}
