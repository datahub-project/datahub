#!/bin/bash

source .env

uvicorn datahub_monitors.app.server:app --reload --port 9004
