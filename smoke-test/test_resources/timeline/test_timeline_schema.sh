#!/bin/bash

datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --hard --force
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a schemaMetadata -d newschema.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a schemaMetadata -d newschemav2.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a schemaMetadata -d newschemav3.json
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --category technical_schema --start 10daysago --end 2682397800000 $1

