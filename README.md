# ADFLogsParser

Usage:`go run main.go -log_path ~/foo.log -output_path ~/bar.txt`

## Options
`-log_path`: The input file to read logs from, can be zipped or unzipped.

`-output_path`: the output text file the summary will be written to.

`-list_files`: whether to include a full list of files in the output file. Defaults to false and will simply state how many files it read. 
