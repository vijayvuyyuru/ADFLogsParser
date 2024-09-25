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
	"time"

	"github.com/edaniels/golog"
	"github.com/pkg/errors"
	"go.viam.com/utils"
)

var (
	logger    = golog.NewDebugLogger("log_parsing")
	listFiles = false
)

type arguments struct {
	LogPath    string `flag:"log_path,usage=path of log file (optionally gzipped), default to STDIN"`
	OutputPath string `flag:"output_path,usage=output file path, default to STDOUT"`
	ListFiles  bool   `flag:"list_files,usage=whether to include a full list of files parsed in output,default=false"`
}

type queryData struct {
	query         map[string]any
	startTime     time.Time
	correlationID string
	files         []string
	duration      string
}

func (q queryData) String() string {
	var sb strings.Builder
	bytes, _ := json.MarshalIndent(q.query, "", "    ")

	sb.WriteString("query:")
	sb.Write(bytes)
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("start time: %s\n", q.startTime))
	sb.WriteString(fmt.Sprintf("duration: %s\n", q.duration))
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

func printReport(queries []queryData, f io.StringWriter) error {
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
	var err error

	var argsParsed arguments
	if err = utils.ParseFlags(args, &argsParsed); err != nil {
		return err
	}
	listFiles = argsParsed.ListFiles

	in := (io.ReadCloser)(os.Stdin)

	if argsParsed.LogPath != "" {
		in, err = os.Open(argsParsed.LogPath)
		defer utils.UncheckedErrorFunc(in.Close)
		if err != nil {
			return err
		}

		if strings.HasSuffix(argsParsed.LogPath, ".gz") {
			gzReader, err := gzip.NewReader(in)
			defer utils.UncheckedErrorFunc(gzReader.Close)
			if err != nil {
				return err
			}

			in = gzReader
		}
	}

	scanner := bufio.NewScanner(in)
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

			t, err := time.Parse(time.RFC3339, decodedData["internalTimestamp"].(string))
			if err != nil {
				return err
			}

			activeQuery = queryData{
				query:         query.(map[string]any),
				startTime:     t,
				correlationID: id,
			}
		} else if strings.Contains(text, "open partition") {
			activeQuery.files = append(activeQuery.files, decodedData["source"].(string))
		} else if strings.Contains(text, "command execution complete") {
			activeQuery.duration = decodedData["elapsed"].(string)
		}
	}
	queries = append(queries, activeQuery)

	out := os.Stdout
	if argsParsed.OutputPath != "" {
		out, err = os.Create(argsParsed.OutputPath)
		defer utils.UncheckedErrorFunc(out.Close)

		if err != nil {
			return err
		}
	}

	err = printReport(queries, out)
	if err != nil {
		return err
	}

	if len(queries) == 1 && queries[0].correlationID == "" {
		logger.Warn("No query found in logs")
	}
	return scanner.Err()
}
