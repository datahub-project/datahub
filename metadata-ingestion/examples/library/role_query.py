import os

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create a DataHub REST emitter
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

# Query a role entity by URN
role_urn = "urn:li:role:snowflake_reader_role"

# Get the role entity
role_entity = emitter._session.get(
    f"{emitter._gms_server}/entities/{role_urn.replace(':', '%3A').replace('(', '%28').replace(')', '%29')}"
)

if role_entity.status_code == 200:
    role_data = role_entity.json()
    print(f"Role URN: {role_data.get('urn')}")

    # Extract role properties
    if "aspects" in role_data:
        aspects = role_data["aspects"]

        # Role properties
        if "roleProperties" in aspects:
            props = aspects["roleProperties"]["value"]
            print(f"Name: {props.get('name')}")
            print(f"Description: {props.get('description')}")
            print(f"Type: {props.get('type')}")
            print(f"Request URL: {props.get('requestUrl')}")

        # Actors (users and groups)
        if "actors" in aspects:
            actors = aspects["actors"]["value"]
            if "users" in actors:
                print(f"Users: {[u['user'] for u in actors['users']]}")
            if "groups" in actors:
                print(f"Groups: {[g['group'] for g in actors['groups']]}")
else:
    print(f"Failed to retrieve role: {role_entity.status_code}")
