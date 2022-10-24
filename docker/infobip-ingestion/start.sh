#!/bin/bash

set -e

VALID_ARGS=$(getopt -o l:r:h --long recipe-list-file:,recipe-file:,help -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

allExtractedRecipes=()
eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -l | --recipe-list-file)
        echo "--- Adding follwoing recipes which were found in '$2':"
        #recipes=$(<$2)
        IFS=$'\r\n' GLOBIGNORE='*' command eval  'recipes=($(cat $2))'
        printf "%s\n" "${recipes[@]}"
        allExtractedRecipes+=( "${recipes[@]}" )
        #echo "Final recipes list:"
        #printf "%s\n" "${allExtractedRecipes[@]}"
        shift 2
        ;;
    -r | --recipe-file)
        echo "--- Adding recipe: $2"
        recipe=$2
        allExtractedRecipes+=( "${recipe}" )
        #echo "Final recipes list:"
        #printf "%s\n" "${allExtractedRecipes[@]}"
        shift 2
        ;;
    -h | --help)
        echo "available keys:"
        echo "  -l | --recipe-list-file - file which contains list of recipe file names/paths (one name per one line)"
        echo "  -r | --recipe-file      - recipe file name/path"
        echo "example: ./start.sh -l recipe-list-file1.txt  -r recipe2.yml -l recipe-list-file3.txt"
        exit 0
        ;;
    --) shift;
        break
        ;;
  esac
done

echo "--- Final recipes list:"
printf "%s\n" "${allExtractedRecipes[@]}"

for recipeFile in "${allExtractedRecipes[@]}"
do
  echo "--- Executing recipe: '$recipeFile'"
  /datahub-src/metadata-ingestion/venv/bin/datahub ingest -c $recipeFile
  echo "--- Executing recipe: '$recipeFile' succeeded"
done
