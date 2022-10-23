#!/bin/bash
cp dags/*.proto .
find . -type f -iname '*.pb' | xargs -I '{}' tar cvzf '{}'.tgz '{}' *.proto 
