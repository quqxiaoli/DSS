#!/bin/bash
for i in {1..100}; do
    curl -X POST "https://localhost:8080/set?api_key=my-secret-key" -d "{\"key\":\"key$i\",\"value\":\"value$i\"}" -k
done
