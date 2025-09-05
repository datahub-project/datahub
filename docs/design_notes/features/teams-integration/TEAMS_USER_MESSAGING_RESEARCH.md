# Microsoft Teams app DM capabilities: Activity feed notifications are the way forward

Microsoft's official stance strongly discourages direct messaging from Teams apps in favor of activity feed notifications. While DMs are technically possible through Bot Framework proactive messaging, Microsoft actively steers developers away from this approach, considering excessive DMs as spam. For your Data Hub notification system, the recommended path combines activity feed notifications with interactive bot capabilities for a superior user experience.

## Microsoft's clear preference against app DMs

Microsoft's documentation and Teams Store validation policies reveal a consistent message: **avoid direct messages for app notifications**. The company explicitly prohibits practices like sending welcome messages to individual users, calling it "spamming." Apps that send excessive DMs face rejection from the Teams Store, with strict limits of **10 notifications per minute per user** enforced through automatic throttling.

The anti-spam policies are particularly stringent. Bots must not send proactive 1:1 messages to users who haven't interacted with them first. Multiple messages in quick succession are forbidden, as are notification-only bots that provide minimal value. Microsoft's vision clearly favors centralized, user-controlled notification management through the activity feed system rather than direct messaging.

## Technical implementation options exist but come with limitations

Despite Microsoft's discouragement, three main technical approaches enable Teams apps to message users directly:

**Bot Framework Proactive Messaging** offers the most reliable method for sending DMs. After a user first interacts with your bot, you store their conversation reference and can send messages anytime. This requires maintaining conversation references with user IDs, tenant IDs, and service URLs. The implementation involves capturing user information during the initial interaction and using the Bot Adapter to send proactive messages later.

**Microsoft Graph API** provides another route but with significant restrictions. Direct messaging through Graph requires **delegated permissions only** - meaning a user must be actively signed in. Application-only permissions don't support sending chat messages, making this approach unsuitable for background notification systems. The API enforces strict rate limits of 4 requests per second per app on a given team.

**Activity Feed Notifications** represent Microsoft's preferred alternative. These appear in the Teams activity feed with OS-level toast notifications, support rich content through adaptive cards, and allow users granular control over notification preferences. Implementation uses the Graph API endpoint `/users/{user-id}/teamwork/sendActivityNotification` with more generous rate limits of 20 notifications per minute per user.

## Permission complexity requires careful planning

The permission landscape for Teams messaging reveals significant complexity. Bot applications automatically receive basic messaging privileges when installed, but Graph API operations require explicit Azure AD permissions. The distinction between delegated and application permissions becomes critical - direct messages through Graph need delegated permissions with active user sessions, while activity feed notifications support both models.

**Resource-Specific Consent (RSC)** offers a more targeted approach, allowing team owners to grant permissions scoped to specific resources rather than organization-wide access. This reduces security risks and simplifies the consent process. However, for direct messaging scenarios, you'll still need to implement proper OAuth flows with token management, refresh strategies, and secure storage.

The "handshake" process mentioned involves multiple steps: bot registration in Azure, Teams app manifest configuration declaring required permissions, user or admin consent during installation, and runtime authentication with HMAC signatures for message validation. Organizations can configure whether users can consent to app permissions individually or require admin approval for all apps.

## Leading apps choose activity feed over DMs

Analysis of successful Teams apps reveals a clear pattern: **major enterprise applications avoid DMs for notifications**. Jira Cloud, ServiceNow, GitHub, and Salesforce integrations all primarily use activity feed notifications or channel messages rather than direct messages.

**Jira Cloud** implements a comprehensive subscription system where users configure notifications via commands like "@Jira Cloud Notifications." The app uses activity feed for most updates, with actionable cards allowing users to comment or edit directly from Teams. **GitHub's integration** follows a similar pattern with repository subscriptions managed through commands like "@github subscribe owner/repo," delivering updates through channels and activity feeds with granular filtering options.

**ServiceNow** takes a multi-tiered approach combining activity feed notifications with interactive bot capabilities for approvals and incident management. Users receive actionable notifications they can respond to directly, but these appear in the activity feed rather than as direct conversation threads. This pattern of activity feed plus interactive bot represents the most successful architecture for notification systems.

## Data Hub should embrace the activity feed pattern

For your Data Hub notification system, I recommend adopting the **Activity Feed + Interactive Bot hybrid pattern** used by successful enterprise apps. This approach aligns with Microsoft's vision while providing excellent user experience and avoiding the pitfalls of direct messaging.

**Architecture recommendations:**

- Use Microsoft Graph Activity Feed APIs for dataset update notifications
- Implement Bot Framework for interactive scenarios requiring user responses
- Create subscription management through commands like "@DataHub subscribe dataset-name"
- Support tag-based filtering and role-based default subscriptions
- Batch notifications to respect rate limits while maintaining performance

**Implementation approach:**

- Store user preferences and subscriptions in your backend
- Use Azure Functions to process data changes and trigger notifications
- Send activity feed notifications for informational updates (new datasets, schema changes)
- Deploy interactive bot messages only for critical actions (approval requests, data validation)
- Provide granular notification controls through a settings interface

**User experience benefits:**

- Users maintain control over notification frequency and types
- Notifications appear in the centralized Teams activity feed
- OS-level notifications ensure visibility without creating conversation clutter
- Deep links can navigate users directly to relevant data assets
- Adaptive cards provide rich, actionable content within notifications

## Key technical details for implementation

**Activity Feed Notification Implementation:**

```javascript
POST https://graph.microsoft.com/v1.0/users/{user-id}/teamwork/sendActivityNotification
{
  "topic": {
    "source": "entityUrl",
    "value": "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/{teamsAppId}"
  },
  "activityType": "datasetUpdated",
  "previewText": {
    "content": "New data available: Sales Q4 2025"
  },
  "recipient": {
    "@odata.type": "microsoft.graph.aadUserNotificationRecipient",
    "userId": "{user-aad-id}"
  },
  "templateParameters": [
    {
      "name": "datasetName",
      "value": "Sales Q4 2025"
    }
  ]
}
```

**Required Permissions:**

- `TeamsActivity.Send.User` (RSC permission for sending to specific users)
- `TeamsActivity.Send` (for broader notification scenarios)
- `User.Read.All` (to resolve user identities)

**Bot Framework Setup for Interactive Scenarios:**

1. Register bot in Azure Bot Service
2. Configure Teams channel
3. Add bot to app manifest with appropriate scopes
4. Implement conversation reference storage on first interaction
5. Use stored references for follow-up interactive cards

## Compliance and governance considerations

Microsoft's approach prioritizes **user control and organizational governance**. The activity feed pattern supports compliance requirements better than DMs:

- **GDPR compliance** through centralized notification management and user preferences
- **Audit trails** automatically maintained in activity feed
- **Admin controls** for notification policies at the organizational level
- **Data residency** handled through Microsoft's regional infrastructure
- **Retention policies** applied consistently to activity feed items

For financial services or healthcare organizations, activity feed notifications integrate better with existing compliance frameworks and eDiscovery processes than scattered DM conversations.

## Conclusion

While Microsoft Teams technically supports direct messaging from apps, the platform's architecture, policies, and successful app examples all point toward **activity feed notifications as the superior approach** for your Data Hub notification system. This pattern provides better user experience, easier compliance management, simpler technical implementation, and alignment with Microsoft's product vision. By adopting the activity feed + interactive bot hybrid approach, your Data Hub will deliver effective notifications while avoiding the pitfalls and restrictions associated with direct messaging.
