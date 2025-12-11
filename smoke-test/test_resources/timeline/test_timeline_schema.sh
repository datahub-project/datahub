#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --hard --force
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a schemaMetadata -d newschema.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a schemaMetadata -d newschemav2.json
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" -a schemaMetadata -d newschemav3.json
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:hive,testTimelineDataset,PROD)" --category technical_schema --start 10daysago --end 2682397800000 $1

