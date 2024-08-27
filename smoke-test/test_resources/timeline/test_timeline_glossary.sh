#!/bin/bash

datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --hard --force
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a glossaryTerms -d newglossary.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a glossaryTerms -d newglossaryv2.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a glossaryTerms -d newglossaryv3.json
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --category glossary_term --start 10daysago --end 2682397800000 $1