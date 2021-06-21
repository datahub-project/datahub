**How to run the API - current edition**  
Make sure u have a python interpreter (compulsory) and fresh environment (optional), then following the steps:    
1. `cd ../docker/ && docker-compose -p datahub up` (recommend to build and up docker-compose to prevent mixing up of old code and newer images)
2. `cd ../metadata-ingestion && pip install --use-feature=in-tree-build .[all] fastapi[all]` (cd to metadata-ingestion and pip install from there ensures we're using the current version of the library)  
3. `cd ../ingest-api && python app.py`
4. navigate to http://localhost:8001/docs to see the API.  

**How to run the API - future edition**
1. `cd ../docker/ingest-api && docker-compose up` (wait for it to build)
2. navigate to http://localhost:8001/docs to see the Swagger API or http://localhost:8001/redoc.  

**Notes**  
ports can be changed by modifying value in app.py (something to shift to .env file i guess)





