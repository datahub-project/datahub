#!/bin/bash
echo "Generating .env file..."
op inject -i ./secrets-env-vars.sh -o .env
echo "File generation complete!"

