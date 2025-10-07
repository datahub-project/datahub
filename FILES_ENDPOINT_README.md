# DataHub Files Endpoint Configuration

The new `/api/files/<file_id>` endpoint has been successfully implemented. Here's what was added:

## Files Added

1. **FilesController** (`metadata-service/graphql-servlet-impl/src/main/java/com/datahub/files/FilesController.java`)
   - REST controller that handles GET requests to `/api/files/{fileId}`
   - Validates UUID format for file IDs
   - Generates presigned S3 URLs and returns HTTP redirects
   - Configurable expiration times (default: 1 hour, max: 7 days)

2. **S3UtilFactory** (`metadata-service/factories/src/main/java/com/linkedin/gms/factory/s3/S3UtilFactory.java`)
   - Spring factory to create S3Util beans
   - Supports both default AWS credentials and STS role assumption
   - Conditionally enabled via configuration

3. **Updated GraphQLServletConfig** 
   - Added component scanning for `com.datahub.files` and `com.linkedin.gms.factory.s3` packages

## Configuration

To enable the files endpoint, add these properties to your `application.yaml`:

```yaml
datahub:
  files:
    s3:
      enabled: true
      # Optional: Use STS role assumption for S3 access
      executorRoleArn: "arn:aws:iam::123456789012:role/DataHubS3AccessRole"
```

## How It Works

1. **Request Flow**: 
   - Frontend makes request to `/api/files/{fileId}`
   - datahub-frontend proxies to metadata service via existing `/api/*` routing
   - FilesController validates the UUID and generates presigned S3 URL
   - Returns HTTP 302 redirect to the presigned URL

2. **S3 Integration**:
   - Uses existing S3Util class from datahub-graphql-core
   - Supports both default AWS credentials and STS role assumption
   - Presigned URLs are valid for configurable duration (1 hour to 7 days)

## Next Steps

You mentioned you need to do additional work in S3Util to generate the correct download URL. The current implementation assumes:

- You have a way to map `fileId` (UUID) to S3 bucket and key
- You have proper S3 configuration and credentials
- The placeholder mapping in FilesController needs to be replaced with your actual file lookup logic

## Example Usage

```bash
# Request a file download
curl -X GET "http://localhost:8080/api/files/123e4567-e89b-12d3-a456-426614174000"

# With custom expiration (2 hours)
curl -X GET "http://localhost:8080/api/files/123e4567-e89b-12d3-a456-426614174000?expiration=7200"
```

The endpoint will return a 302 redirect to the presigned S3 URL, which the browser will automatically follow to download the file.
