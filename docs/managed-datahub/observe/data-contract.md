# Data Contracts

## A Data Contract is…

- Verifiable : based on the actual physical data asset, not its metadata (eg. schema checks, column-level data checks, and operational SLA-s but not documentation, ownership, and tags).
- A set of assertions : The actual checks against the physical asset to determine a contract’s status (schema, freshness, volume, custom, and column)
- Producer oriented : One contract per physical data asset, owned by the producer.\[collapse section]> Consumer oriented data contractsWe’ve gone with producer-oriented contracts to keep the number of contracts manageable and because we expect consumers to desire a lot of overlap in a given physical asset’s contract. Although, we've heard feedback that consumer-oriented data contracts meet certain needs that producer-oriented contracts do not. For example, having one contract per consumer all on the same physical data asset would allow each consumer to get alerts only when the assertions they care about are violated.We welcome feedback on this in slack!Validated Data Contract example![](https://lh7-us.googleusercontent.com/m2MfgNq5E9t51NjtI-rquwfaRWCeNJgVbcbD6XrU0aC-nwx-gLUo0Td680oq5c5IkCB-se44qReRWeHryaKbYxq7k-fGhJMZMIzRYCsU1gQkpew-zWRx-r7kxN7VoLzXJ0H8_svLp6VTKhUOKHTPkD8)**

## Data Contract and Assertions

Another way to word our vision of data contracts is: _A bundle of verifiable assertions on physical data assets representing a public producer commitment._These can be all of the assertions on an asset or only the subset you want publicly promised to consumers. Data Contracts allow you to “promote” a selected group of your assertions as a public promise: if this subset of assertions is not met, the Data Contract is failing.A note on ownership - the owner of the physical data asset is also the owner of the contract and can accept proposed changes and make changes themselves to the contract.

## How to Create Data Contracts

Data Contracts can be created via DataHub CLI (YAML), API, or UI.

### DataHub CLI using YAML

For creation via CLI, it’s a simple CLI upsert command that you can integrate into your CI/CD system to publish your Data Contracts and any change to them.
1) Define your data contract.
```yaml
# id: sample_data_contract # Optional: if not provided, an id will be generated
entity: urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)
version: 1
freshness:
  type: cron
  cron: "4 8 * * 1-5"
schema:
    type: json-schema
    json-schema:
        type: object
        properties:
        field_foo:
            type: string
            native_type: VARCHAR(100)
        field_bar:
            type: boolean
            native_type: boolean
        field_documents:
            type: array
            items:
            type: object
            properties:
                docId:
                type: object
                properties:
                    docPolicy:
                    type: object 
                    properties:
                        policyId:
                        type: integer
                        fileId:
                        type: integer
        required:
        - field_bar
        - field_documents


```

### UI

1. Navigate to the Dataset Profile for the dataset you wish to create a contract for
2. Under the **Validations** > **Data Contracts** tab, click **Create**.![](https://lh7-us.googleusercontent.com/aOIfU9hAnJA4j_ii1F_qKBezbAxUJil8Y8mq7cr3Le0l5MjNPkt6VXkhJSEtVRU83zBa8fR9lmhPoilhCDjU7x7OZ7vEpP4BUyS3OUWz2M9HG9cv1ROzAjhktbltvz5gLaISvcf2q0DKWsiOnjcEukM)
3. Select the assertions you wish to be included in the Data Contract. (_Note that when creating a Data Contract via the UI, the Freshness, Schema, and Data Quality assertions are expected to have been created already)_![](https://lh7-us.googleusercontent.com/UpHZ20vpUJAPgS6Jos1zviSu1nLkZl7a1s40Zd3HE3GeN2lNbom37sJCK4K_q4O-BUmwdUk2sS35PRu_ZnmKgjwDlUTdIh3aXFIXv1mydZm9BRVOeMwnfsFWDfFOovQzizgv6M3ZMa-IfjEyS8sGdXc)
4. See it in the UI![](https://lh7-us.googleusercontent.com/hoDoFEuHmTC8M1LhgbwvE6FWn9F0s3N-JZFlmDouALVlsOjHBcwLnCAPHD2gtLAb5n83-FRA0oqw1FIoWyLmOx7RyejukGJPqdqHwmzUPhnisYC8NXqdeVyP8SqdwtUYTBDMsw2C0N_PO-w908geWZQ)**

### API

Coming soon.

## How to Run Data Contracts
