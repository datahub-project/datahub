**How to run the API - current edition**
1. `cd ../docker/ingest-api && docker-compose up` (wait for it to build)
2. navigate to http://localhost:8001/docs to see the Swagger API or http://localhost:8001/redoc.  

**Future improvement Notes**  
1. ports can be changed by modifying value in app.py (something to shift to .env file i guess)  
2. I want to install all the extensions for the metadata-ingest python library, but now got issue installing the sasl library (Pyhive library)  
3. Currently logs cannot capture a request that is rejected by Pydantic validation. 

**Sample curl commands to api**
`curl -X GET http://localhost:8001/hello` hello world should return something  

make a sample dataset:  
`curl -d '{"dataset_name":"dataset_name", "dataset_type":"text/csv", "dataset_owner": "34552", "dataset_description": "hello this is desc", "dataset_fields": [{"field_name":"field1","field_type":"string", "field_description":"col1"}], "dataset_origin":"origin", "dataset_location":"location"}' -H "Content-Type: application/json" -X POST http://localhost:8001/make_dataset`  

the api will return a string containing the url if successful.

note:
inside datahub/datahub-web-react/src/conf/Adhoc.ts:
change to `const config = 'http://localhost:8001/make_dataset';`




