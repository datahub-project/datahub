import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Microsoft Teams App

<FeatureAvailability saasOnly />

The DataHub Microsoft Teams App brings DataHub's powerful data discovery, AI assistance, and collaboration features directly into your Teams workspace. Once installed, you can search for data assets, get AI-powered insights, and stay updated on data changes without leaving Teams.

## Features

### Slash Commands

Use `/datahub` commands to interact with DataHub directly in Teams:

#### Search for Data Assets

```
/datahub search customer data
/datahub search orders table
/datahub search "user activity"
```

Find datasets, dashboards, data jobs, and other data assets across your organization.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/search_command.png"/>
</p>

#### Get Entity Details

```
/datahub get urn:li:dataset:(urn:li:dataPlatform:snowflake,schema.table,PROD)
```

Retrieve detailed information about a specific data asset using its URN.

#### Get Help

```
/datahub help
```

Display available commands and usage instructions.

### AI-Powered Data Assistant

#### @Mention the DataHub Bot

Ask questions about your data by @mentioning the DataHub bot:

```
@DataHub What tables contain customer information?
@DataHub How is revenue calculated in our data warehouse?
@DataHub Show me recent changes to the users table
@DataHub What's the data lineage for our sales dashboard?
```

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/ai_assistant.png"/>
</p>

The AI assistant provides:

- **Intelligent Responses**: Context-aware answers about your data landscape
- **Follow-up Suggestions**: Clickable buttons for related questions
- **Live Progress Updates**: Real-time feedback during processing
- **Entity Links**: Direct links to relevant data assets in DataHub

### URL Unfurling

When you paste a DataHub entity URL in Teams, the bot automatically unfurls it with rich information:

- Entity name and description
- Key properties and metadata
- Direct link to view in DataHub
- Owner and contact information

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/url_unfurling.png"/>
</p>

### Notifications Integration

Receive DataHub notifications directly in Teams channels:

- **Data Quality Alerts**: Get notified when data quality checks fail
- **Schema Changes**: Be alerted to schema modifications
- **Ownership Changes**: Know when data asset ownership changes
- **Incident Updates**: Stay informed about data incidents

Configure notifications in your DataHub settings to route them to specific Teams channels.

## Usage Examples

### Data Discovery Workflow

1. **Search for relevant data**:

   ```
   /datahub search customer analytics
   ```

2. **Ask follow-up questions**:

   ```
   @DataHub What's the data quality of the customer_analytics table?
   ```

3. **Get detailed information**:
   - Click on any result to view full details in DataHub
   - Use the AI assistant for deeper insights

### Collaboration Scenarios

#### Team Channel Discussion

```
Team Member: "We need customer data for the Q4 analysis"
You: /datahub search customer data
DataHub Bot: [Shows search results with customer datasets]
Team Member: "Perfect! What about data quality?"
You: @DataHub What's the data quality status of customer_profiles?
DataHub Bot: [Provides quality metrics and recent test results]
```

#### Incident Response

```
Alert: "Data quality check failed on orders_table"
Team Lead: @DataHub What happened with the orders table?
DataHub Bot: [Explains the failed assertion and recent changes]
Team Lead: "Who owns this table?"
[DataHub bot provides owner information with contact details]
```

## Best Practices

### Effective Search Queries

- Use specific terms: `"user sessions"` instead of just `sessions`
- Include platform names: `snowflake customer data`
- Use business terms: `revenue` instead of technical names

### AI Assistant Tips

- Ask specific questions for better responses
- Use follow-up suggestions to explore related topics
- Mention specific table or dataset names when possible
- Ask about data lineage, quality, or ownership for comprehensive insights

### Team Collaboration

- Use Teams channels for data discussions
- Share DataHub URLs for easy entity access
- Set up notification channels for different data domains
- Use @mentions for complex questions that benefit from AI analysis

## Permissions and Privacy

The DataHub Teams app respects your organization's data governance:

- **Data Access**: Users can only see data they have permission to access in DataHub
- **Search Results**: Filtered based on individual user permissions
- **Entity Details**: Only show information accessible to the requesting user
- **Secure Communication**: All interactions use secure Microsoft Teams channels

## Troubleshooting

### Common Issues

**Commands not working**

- Ensure the app is installed in your personal Teams app list
- Check that your Teams admin has published the app
- Verify you're using the correct command format

**AI assistant not responding**

- Check that @mentions include the correct bot name
- Ensure you're in a channel where the bot has been added
- Try rephrasing your question more specifically

**Search returns no results**

- Verify you have access to the data in DataHub
- Try broader search terms
- Check with your DataHub admin about data availability

**URL unfurling not working**

- Ensure the URL is a valid DataHub entity link
- Check that the bot can access the shared link
- Verify the entity exists and you have permission to view it

### Getting Help

- Type `/datahub help` for command reference
- Contact your DataHub administrator for access issues
- Check your Teams admin for app installation problems
- Refer to the [Teams troubleshooting guide](saas-teams-troubleshoot.md) for detailed solutions

## Next Steps

- Set up [notification subscriptions](../subscription-and-notification.md) to get Teams alerts
- Explore the [DataHub web interface](https://docs.datahub.com/docs/quickstart) for advanced features
- Configure [data governance policies](https://docs.datahub.com/docs/authorization/policies) for your team
- Learn about [data discovery best practices](https://docs.datahub.com/docs/how/search) in DataHub
