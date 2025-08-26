#!/bin/bash

source .env

uvicorn datahub_executor.coordinator.server:app --reload --port 9004
