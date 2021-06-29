***Keycloak container***    

**to run for first time (if not intending to use the json provided)**  
first setup a keycloak container (container name: keycloak) with all the users and settings setup. Make sure DH can run properly with it.
Copy the script into the container:  
`docker cp docker-exec-cmd.sh keycloak:/tmp/docker-exec-cmd.sh`  
Execute the script inside of the container  
`docker exec -it keycloak /tmp/docker-exec-cmd.sh`  
view and copy out the json file  
`cat /tmp/realms-export-single-file.json`  

**if intending to just use the provided script and run:**  
1. at datahub/docker folder: `./create_network.sh`
2. at datahub/docker folder: `docker-compose -p datahub up -d`
3. at datahub/docker/keycloak folder: `docker-compose up -d`  
keycloak will startup and import the json script if the realm does not already exist in its volume.  

**about the docker-compose and keycloak settings**  
It creates the container and a volume. Attaches to the datahub_network.  
Keycloak runs on 172.19.0.1:8088 and expects to find datahub-frontend at 172.19.0.1:9002  
172.19.0.1 is the gateway ip for the datahub_network.

**users in the app realm:**  
*login/password info*  
`demo/password`  
`sysadmin/password`  
`datahub/password`  

reference:  
extracts realm info from container into json form.
from: https://stackoverflow.com/questions/60766292/how-to-get-keycloak-to-export-realm-users-and-then-exit
What it can do:
copies out all the information and settings in a particular realm. For testing, I created an "app" realm and it has all my users and client settings.  
Master realm does not have any settings deployed other than adding App realm.