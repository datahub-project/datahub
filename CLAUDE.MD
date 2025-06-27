# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) or any other agent when working with code in this repository.

## Coding conventions

- Keep code maintainable. This is not throw-away code. This goes to production. 
- Generate unit tests where appropriate. 
- Do not start generating random scripts to run the code you generated unless asked for.
- Do not add comments which are redundant given the function names

## Core concept docs

 - `docs/what/urn.md` defines what a URN is

## Directory structure

- This is repository for DataHub project.
- `README.MD` should give some basic information about the project.
- This is a multi-project gradle project so you will find a lot of `build.gradle` in most folders
- `metadata-ingestion` contains source and tests for DataHub OSS CLI. 
    - `pyproject.toml`, `setup.py`, `setup.cfg` in the folder contain rules about the code style for the repository
    - The `.md` files at top level in this folder gives you important information about the concepts of ingestion
