---
title: File Upload and Download in Documentation
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# File Upload and Download in Documentation

<FeatureAvailability />

DataHub's File Upload and Download capability enables you to enrich your asset and column documentation with supporting files like images, diagrams, and other resources, making your data catalog more informative and easier to understand.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/file_upload_download/drop_file_upload.png"
       alt="Dropping a file to upload"/>
</p>

## Why Use File Uploads in Documentation?

File uploads transform how you document and communicate about your data assets by enabling you to:

- **Enhance Documentation Quality**: Add visual diagrams, architecture drawings, and screenshots directly into your documentation to illustrate complex concepts
- **Centralize Resources**: Keep all relevant documentation materials in one place alongside your data assets rather than scattered across multiple systems
- **Improve Understanding**: Help users grasp data structures, pipelines, and relationships faster with visual aids embedded directly in context
- **Standardize Communication**: Create consistent, professional documentation that combines text and visual elements

Whether you're a data engineer documenting a complex data pipeline, a data analyst explaining column definitions with example screenshots, or a data steward providing reference materials, file uploads make your documentation more comprehensive and accessible.

## What's Included

### Supported File Types

Currently, images are displayed inline within your documentation. Files of other types can be uploaded and downloaded, with support for additional inline previews (like PDFs and text files) coming in future releases.

### Upload Methods

You have two convenient ways to add files to your documentation:

- **Drag and Drop**: Simply drag files from your file system directly into the documentation editor
- **Upload Button**: Click the upload file button in the editor toolbar to browse and select files from your file system

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/file_upload_download/upload_file_button.png"
       alt="Option to choose a file from your system"/>
</p>

### File Management

When you upload a file, DataHub automatically:

- Creates a new `dataHubFile` entity to track metadata about your file (type, size, original filename)
- Stores the context of where you uploaded the file (asset documentation, column documentation, etc.)
- Uses this context information to enforce permissions when users attempt to download files

Files are securely stored in S3, and access is controlled based on the user's permissions for the asset where the file was uploaded.

## Setting Up File Uploads

File upload functionality requires configuration to connect DataHub to your S3 storage.

### Prerequisites

Your DataHub instance must be deployed on AWS for the AWS role authentication to work seamlessly.

### Required Configuration

Set the following environment variables in your DataHub GMS service:

#### DATAHUB_BUCKET_NAME

The name of your S3 bucket where uploaded files will be stored.

```bash
DATAHUB_BUCKET_NAME=your-datahub-files-bucket
```

#### DATAHUB_ROLE_ARN

The AWS role ARN configured with permissions to upload and download files from your S3 bucket.

```bash
DATAHUB_ROLE_ARN=arn:aws:iam::123456789012:role/DataHubFileAccessRole
```

### AWS Role Configuration

Your AWS role must be properly configured with two key requirements:

#### S3 Permissions

The role needs appropriate permissions to interact with your S3 bucket. At minimum, this should include:

- `s3:PutObject` - To upload files
- `s3:GetObject` - To download files
- `s3:DeleteObject` - For garbage collection of unused files

Configure these permissions through an IAM policy attached to the role.

#### Trust Relationship

Update the trust relationship for your role to allow your DataHub GMS service to assume it. The trust policy should permit the AWS service or role that your DataHub GMS is running under to assume this role.

Without proper trust relationship configuration, DataHub will not be able to authenticate with AWS to access your S3 bucket.

## Uploading Files

To add files to your documentation:

1. Navigate to the asset or column where you want to add documentation
2. Open the documentation editor
3. Either:
   - Drag and drop your file directly into the editor, or
   - Click the upload file button in the toolbar and select your file from the file system dialog
4. Images will appear inline immediately; other file types will show as downloadable links
5. Save your documentation changes

Files are uploaded to S3 as soon as you add them to the editor, even before you save your documentation changes.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/file_upload_download/image_file_example.png"
       alt="Image file rendered inline"/>
</p>

## Downloading Files

When users view documentation containing uploaded files:

- **Images**: Display inline automatically within the documentation
- **Other Files**: Appear as download links that users can click to retrieve the file

### Permission Checks

When a user attempts to download a file, DataHub verifies that they have permission to view the asset or column where the file was originally uploaded. This ensures that file access respects your existing DataHub permission structure.

If a user doesn't have permission to view the associated asset, they won't be able to download the file, even if they have a direct link to it.

## File Metadata and Tracking

Each uploaded file is represented as a `dataHubFile` entity in DataHub, which stores:

- **File type**: The MIME type of the uploaded file
- **File size**: Size in bytes
- **Original filename**: The name of the file when it was uploaded
- **Upload context**: The entity (asset or column) where the file was added

This metadata enables future features like file search, usage tracking, and cleanup of orphaned files.

## Best Practices

When using file uploads in your documentation:

- **Use descriptive filenames**: Original filenames are preserved, making it easier to identify files later
- **Optimize image sizes**: Large images may take longer to load; consider resizing before upload
- **Provide context**: Add text descriptions around images to explain what users are seeing
- **Consider alternatives for large files**: For very large files, consider linking to external storage rather than uploading directly
- **Review permissions**: Ensure your S3 role and bucket permissions are configured correctly before enabling this feature for users

## Troubleshooting

### Files Not Uploading

If files fail to upload, check:

- Your `DATAHUB_BUCKET_NAME` is set correctly and the bucket exists
- Your `DATAHUB_ROLE_ARN` is valid and points to an existing role
- The trust relationship on your AWS role permits your DataHub GMS to assume it
- The role has necessary S3 permissions (`s3:PutObject`)
- Ensure that your S3 bucket has a CORS policy set to accept uploads from your host URL

### Files Not Downloading

If users cannot download files, verify:

- Users have permission to view the asset where the file was uploaded
- Your AWS role has `s3:GetObject` permissions on the bucket
- The file still exists in S3 (hasn't been manually deleted)

### Authentication Issues

If you see AWS authentication errors:

- Confirm your DataHub instance is deployed on AWS
- Verify the trust relationship configuration on your AWS role
- Check that your GMS service has the necessary AWS credentials or instance profile

## Next Steps

Now that you understand how to use file uploads in documentation:

- **Configure your environment**: Set up the required S3 bucket and AWS role
- **Test with images**: Start by uploading images to asset documentation to see how inline display works
- **Train your team**: Show data owners and stewards how to enrich their documentation with visual aids
- **Monitor usage**: Keep an eye on your S3 bucket to understand storage needs
- **Stay tuned**: Watch for upcoming features like PDF previews and additional file type support

File uploads are designed to make your DataHub documentation more comprehensive and user-friendly, helping your organization build a richer, more informative data catalog.
