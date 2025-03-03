

#### Credential to access to GCP
1. Follow the section on credentials to access Vertex AI  [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

#### Create a service account and assign roles

1. Setup a ServiceAccount as per [GCP docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console)
   and assign the previously created role to this service account.
2. Download a service account JSON keyfile.
   Example credential file:

```json
{
   "type": "service_account",
   "project_id": "project-id-1234567",
   "private_key_id": "d0121d0000882411234e11166c6aaa23ed5d74e0",
   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----",
   "client_email": "test@suppproject-id-1234567.iam.gserviceaccount.com",
   "client_id": "113545814931671546333",
   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
   "token_uri": "https://oauth2.googleapis.com/token",
   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%suppproject-id-1234567.iam.gserviceaccount.com"
}
```

3. To provide credentials to the source, you can either:

   Set an environment variable:

   ```sh
   $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
   ```

   _or_

   Set credential config in your source based on the credential json file. For example:

   ```yml
   credential:
     private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
     private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
     client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
     client_id: "123456678890"
   ```
