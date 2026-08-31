import urllib.parse

import requests

gms_server = "http://localhost:8080"

model_urn = "urn:li:mlModel:(urn:li:dataPlatform:mlflow,customer-churn-predictor,PROD)"
encoded_urn = urllib.parse.quote(model_urn, safe="")

response = requests.get(f"{gms_server}/entities/{encoded_urn}")

if response.status_code == 200:
    entity = response.json()

    print(f"Entity URN: {entity['urn']}")
    print("\nAspects:")

    if "mlModelProperties" in entity["aspects"]:
        props = entity["aspects"]["mlModelProperties"]
        print(f"  Name: {props.get('name')}")
        print(f"  Description: {props.get('description')}")
        print(f"  Type: {props.get('type')}")

        if props.get("hyperParams"):
            print("\n  Hyperparameters:")
            for param in props["hyperParams"]:
                print(f"    - {param['name']}: {param['value']}")

        if props.get("trainingMetrics"):
            print("\n  Training Metrics:")
            for metric in props["trainingMetrics"]:
                print(f"    - {metric['name']}: {metric['value']}")

    if "globalTags" in entity["aspects"]:
        tags = entity["aspects"]["globalTags"]["tags"]
        print(f"\n  Tags: {[tag['tag'] for tag in tags]}")

    if "ownership" in entity["aspects"]:
        owners = entity["aspects"]["ownership"]["owners"]
        print(f"\n  Owners: {[owner['owner'] for owner in owners]}")

    if "intendedUse" in entity["aspects"]:
        intended = entity["aspects"]["intendedUse"]
        print(f"\n  Primary Uses: {intended.get('primaryUses')}")
        print(f"  Out of Scope Uses: {intended.get('outOfScopeUses')}")

else:
    print(f"Failed to fetch entity: {response.status_code}")
    print(response.text)
