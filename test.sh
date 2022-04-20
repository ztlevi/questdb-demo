#!/usr/bin/env zsh

export GOBIN=~/dev/questdb-demo/bin/
go install .

./bin/questdb-demo

curl -G \
  --data-urlencode "query=SELECT * FROM gpu limit 2;" \
  http://localhost:9000/exec > out.json
prettier out.json
curl -G \
  --data-urlencode "query=SELECT COUNT(*) FROM gpu;" \
  http://localhost:9000/exec > out.json
prettier out.json

