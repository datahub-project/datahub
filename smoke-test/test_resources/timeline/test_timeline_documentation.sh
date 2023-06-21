#!/bin/bash

datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --hard --force
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a institutionalMemory -d newdocumentation.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a institutionalMemory -d newdocumentationv2.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a institutionalMemory -d newdocumentationv3.json
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --category documentation --start 10daysago --end 2682397800000 $1