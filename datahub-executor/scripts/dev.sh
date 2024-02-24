#!/bin/bash

source .env

uvicorn datahub_executor.coordinator:app --reload --port 9004
