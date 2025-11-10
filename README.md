run `uv sync` first, then run the parser

```
uv run python s3_access_logs_local_parser.py \
  --input ~/Downloads/test-recording-logs/ \
  --csv-out ./s3_logs_parsed.csv \
  --db sqlite \
  --db-path ./s3_logs.sqlite \
  --demo
```

```
uv run python s3_access_logs_local_parser.py \
  --input ~/Downloads/recording-logs-cn/ \
  --csv-out ./s3_logs_parsed_cn.csv \
  --bad-out ./s3_logs_bad_lines_cn.txt \
  --db sqlite \
  --db-path ./s3_logs_cn.sqlite \
  --demo
```

```
update s3_access_logs
   set meetingToken=REPLACE(
         json_extract('["' || REPLACE(key, '/', '","') || '"]', '$[3]'),
         'b.mp4', ''
       )
where key like '2025/%' and key like '%mp4';
```
