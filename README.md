run `uv sync` first, then run the parser

```
uv run python s3_access_logs_local_parser.py \
  --input ~/Downloads/test-recording-logs/ \
  --csv-out ./s3_logs_parsed.csv \
  --db sqlite \
  --db-path ./s3_logs.sqlite \
  --demo
```
