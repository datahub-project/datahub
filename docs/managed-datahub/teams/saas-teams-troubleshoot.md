import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Microsoft Teams Troubleshooting

<FeatureAvailability saasOnly />

This guide helps resolve common issues with the DataHub Microsoft Teams integration.

## App Installation Issues

### App Won't Upload to Teams Admin Center

**Symptoms:**

- Error when uploading the ZIP file
- "Invalid app package" message
- Upload fails silently

**Solutions:**

1. **Check Teams Admin Permissions**

   - Ensure you have Teams Administrator role
   - Verify you can access [admin.teams.microsoft.com](https://admin.teams.microsoft.com)
   - Confirm custom app uploads are enabled

2. **Verify ZIP File Integrity**

   - Re-download the app package from DataHub team
   - Ensure the ZIP file isn't corrupted
   - Check file size (should be ~50KB for typical DataHub Teams app)

3. **Enable Custom App Uploads**
   - Go to **Teams apps** > **Setup policies**
   - Find the policy applied to your users
   - Set **Upload custom apps** to "On"

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/troubleshoot_upload.png"/>
</p>

### App Not Appearing in Teams Store

**Symptoms:**

- App uploaded successfully but users can't find it
- "DataHub" doesn't appear in Teams app store

**Solutions:**

1. **Publish the App**

   - Go to **Teams apps** > **Manage apps**
   - Find "DataHub" in the list
   - Click the app name
   - Click **Publish** or **Available**

2. **Check App Permissions**

   - Go to **Teams apps** > **Permission policies**
   - Ensure the policy allows custom apps
   - Apply policy to affected users/groups

3. **Clear Teams Cache**
   - Users should restart Microsoft Teams
   - Clear Teams cache: `%appdata%\Microsoft\Teams` (Windows) or `~/Library/Application Support/Microsoft/Teams` (Mac)

## Bot Response Issues

### Bot Not Responding to Commands

**Symptoms:**

- `/datahub` commands show no response
- Bot appears offline or inactive

**Solutions:**

1. **Verify Azure Bot Configuration**
   - Check messaging endpoint in Azure Bot Service
   - Ensure endpoint URL matches DataHub webhook URL
   - Confirm Bot Framework Teams channel is enabled

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/troubleshoot_bot_config.png"/>
</p>

2. **Check DataHub Configuration**

   - Verify Azure credentials are correctly configured in DataHub
   - Ensure integrations service is running
   - Check DataHub logs for webhook errors

3. **Test Bot Connectivity**
   - Use Bot Framework Emulator to test bot responses
   - Verify webhook endpoint is publicly accessible
   - Check firewall settings

### @Mentions Not Working

**Symptoms:**

- @DataHub mentions don't trigger responses
- Bot doesn't seem to receive mentions

**Solutions:**

1. **Add Bot to Channel**

   - Type `@DataHub` and add the bot to the channel
   - Ensure bot has permission to read channel messages
   - Check bot appears in channel member list

2. **Verify App Installation**

   - Ensure users have installed the personal app
   - Check app is added to the specific Teams channel
   - Restart Teams application

3. **Check Mention Format**
   - Use exact bot name: `@DataHub`
   - Ensure space after mention: `@DataHub what tables...`
   - Avoid extra characters in mention

## Authentication Issues

### Invalid Credentials Error

**Symptoms:**

- "Authentication failed" messages in logs
- Bot responds with authentication errors

**Solutions:**

1. **Verify Azure AD Credentials**

   - Check App ID, Client Secret, and Tenant ID in DataHub configuration
   - Ensure client secret hasn't expired (check Azure portal)
   - Verify credentials match exactly (no extra spaces)

2. **Check API Permissions**
   - Confirm Microsoft Graph permissions are granted
   - Ensure admin consent was provided
   - Verify permissions include: `User.Read.All`, `TeamMember.Read.All`, `Channel.ReadBasic.All`

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/troubleshoot_permissions.png"/>
</p>

3. **Test Token Generation**
   - Use Azure CLI to test authentication: `az login --service-principal`
   - Verify tenant ID is correct for your organization
   - Check app registration is in correct Azure tenant

### Permission Denied Errors

**Symptoms:**

- "Access denied" when using bot features
- Some commands work but others fail

**Solutions:**

1. **Grant Admin Consent**

   - Go to Azure AD > App registrations > Your app
   - Navigate to API permissions
   - Click "Grant admin consent for [Organization]"

2. **Check User Permissions**

   - Verify users have appropriate DataHub permissions
   - Ensure Teams users are mapped to DataHub users
   - Check data asset permissions in DataHub

3. **Review App Registration**
   - Confirm app registration supports required account types
   - Verify redirect URIs are configured if needed
   - Check certificate/secret expiration dates

## Search and Content Issues

### Search Returns No Results

**Symptoms:**

- `/datahub search` returns empty results
- Search works in DataHub web UI but not Teams

**Solutions:**

1. **Check DataHub Connectivity**

   - Verify integrations service can reach DataHub GMS
   - Test GraphQL endpoint availability
   - Check authentication to DataHub backend

2. **Verify User Permissions**

   - Ensure user has search permissions in DataHub
   - Check if user is authenticated in DataHub
   - Verify Teams user mapping to DataHub user

3. **Test Search Query**
   - Try simpler search terms
   - Test search in DataHub web interface first
   - Check if specific datasets exist

### URL Unfurling Not Working

**Symptoms:**

- DataHub URLs don't expand in Teams
- URLs show as plain text links

**Solutions:**

1. **Check URL Format**

   - Ensure URLs are valid DataHub entity links
   - Verify URL domain matches DataHub instance
   - Test URL accessibility in browser

2. **Bot Link Preview Permissions**

   - Ensure bot has `links:read` and `links:write` permissions in Teams
   - Check if link previews are enabled in Teams settings
   - Verify bot can access the linked content

3. **DataHub Entity Access**
   - Confirm entity exists and is accessible
   - Check user permissions for the entity
   - Verify entity hasn't been deleted

## Performance Issues

### Slow Bot Responses

**Symptoms:**

- Commands take long time to respond
- Timeout errors in Teams

**Solutions:**

1. **Check DataHub Performance**

   - Monitor DataHub backend performance
   - Check search index health
   - Verify database connectivity

2. **Review Integration Service**

   - Check integrations service resource allocation
   - Monitor memory and CPU usage
   - Review application logs for bottlenecks

3. **Optimize Queries**
   - Use more specific search terms
   - Limit search scope when possible
   - Check for large result sets

### AI Assistant Taking Too Long

**Symptoms:**

- @mention responses are very slow
- Progress indicators stop updating

**Solutions:**

1. **Check AI Service Status**

   - Verify AI/LLM service availability
   - Check API rate limits and quotas
   - Monitor AI service response times

2. **Review Question Complexity**
   - Simplify complex questions
   - Break down multi-part questions
   - Use more specific entity references

## Getting Additional Help

### Enable Debug Logging

Contact your DataHub administrator to enable debug logging:

```bash
# Set log level for Teams integration
export DATAHUB_TEAMS_LOG_LEVEL=DEBUG

# Restart integrations service
docker restart datahub-integrations-service
```

### Collect Diagnostic Information

When contacting support, provide:

1. **Azure Configuration**

   - App ID (redact sensitive parts)
   - Tenant ID
   - Bot resource name
   - Teams channel configuration

2. **Error Details**

   - Exact error messages
   - Steps to reproduce
   - Time of occurrence
   - Affected users/channels

3. **Teams Environment**
   - Teams admin center configuration
   - App installation status
   - Permission policies applied
   - Custom app upload settings

### Contact Support

- **DataHub Configuration Issues**: Contact your DataHub team or support
- **Azure/Teams Issues**: Contact your IT administrator or Microsoft support
- **Integration Problems**: Provide logs and configuration details to DataHub support

### Useful Resources

- [Microsoft Teams Admin Documentation](https://docs.microsoft.com/en-us/microsoftteams/teams-overview)
- [Azure Bot Service Documentation](https://docs.microsoft.com/en-us/azure/bot-service/)
- [Microsoft Graph Permissions Reference](https://docs.microsoft.com/en-us/graph/permissions-reference)
- [DataHub Authentication Guide](https://datahub.io/docs/authentication/)
