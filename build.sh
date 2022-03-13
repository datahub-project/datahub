#docker buildx build --platform linux/arm64 -t meshlake/datahub-ingestion:arm64 --push -f ./docker/datahub-ingestion/Dockerfile . 


docker buildx build --platform linux/amd64 -t meshlake/datahub-ingestion:amd64 --push -f ./docker/datahub-ingestion/Dockerfile . 

#docker manifest create  meshlake/datahub-ingestion:latest \
--amend meshlake/datahub-ingestion:amd64 \
--amend meshlake/datahub-ingestion:arm64 \
	 meshlake/datahub-ingestion:latest
