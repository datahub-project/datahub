# A convenience command to run the built docker container with a local config file and local output directory
config_file=$1
echo "Config file is" $config_file
local_dir=$(dirname $config_file)
filename=$(basename $config_file)
output_dir=$(mktemp -d /tmp/ingest.XXXXXX)
echo "Output directory is" $output_dir
docker run --rm --network host \
    --mount type=bind,source="$(pwd)"/$local_dir,target=/injected_dir \
    --mount type=bind,source=$output_dir,target=/output \
    local/dhub-ingest:latest \
    ingest -c /injected_dir/${filename}
