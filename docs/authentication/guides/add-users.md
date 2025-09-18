---
title: Adding Users to DataHub
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Adding Users to DataHub

<FeatureAvailability />

DataHub provides multiple approaches for onboarding users to your organization, with streamlined workflows that make it easy to invite the right people and get them productive quickly.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/invite-users-modal.png"/>
</p>

## Tools to Grow Your DataHub User Base

DataHub's user invitation system helps your organization scale data adoption efficiently by enabling you to:

- **Streamline Onboarding**: Invite users directly via email with pre-assigned [roles](../../authorization/roles.md), eliminating complex manual setup steps
- **Discover Data Power Users**: Automatically identify and invite the most active users from your data platforms based on usage analytics
- **Control Access**: Manage permissions through role-based access control while supporting flexible authentication options
- **Scale Team Adoption**: Generate shareable invite links for bulk onboarding while maintaining governance standards

Whether you're a data platform administrator rolling out DataHub organization-wide, or a team lead wanting to bring specific users into your data workflows, DataHub provides the right invitation method for your approach.

## How to Invite Users to DataHub

DataHub offers the following methods for adding users, with availability depending on your deployment type:

| Invitation Method           | Description                                                         | DataHub Cloud | DataHub Core |
| --------------------------- | ------------------------------------------------------------------- | ------------- | ------------ |
| Email Invitations           | Send personalized invitations directly to users with assigned roles | ✅            | ❌           |
| User Invite Recommendations | Automatically discover and invite power users from usage analytics  | ✅            | ❌           |
| Shared Invite Links         | Generate role-specific shareable links for bulk onboarding          | ✅            | ✅           |
| Static Credential Files     | File-based user management for controlled environments (via JaaS)   | ❌            | ✅           |

Read on to learn more about each of these approaches to bringing users into your DataHub deployment.

### A Note on Authentication

Understanding how DataHub handles user authentication will help you choose the right invitation approach. These authentication methods will be referenced throughout the guide:

- **Single Sign-On (SSO)**: For enterprise deployments, DataHub integrates with identity providers like Azure AD, Okta, and Google via OpenID Connect. When SSO is configured, invited users will authenticate through your organization's existing identity system. See the [DataHub Cloud OIDC Integration](../../managed-datahub/integrations/oidc-sso-integration.md) or [Self-Hosted OIDC Configuration](sso/configure-oidc-react.md) guides for setup details.

- **Native Authentication**: DataHub also supports direct username/password authentication for environments where SSO isn't available or for special access scenarios. For self-hosted deployments requiring file-based user management, see the [JaaS Authentication](jaas.md) guide.

## Email Invitations (DataHub Cloud)

Email invitations allow administrators to directly invite users with pre-assigned roles, creating a personalized onboarding experience.

### How Email Invitations Work

When you invite users via email, DataHub automatically:

- Sends personalized invitation emails with your organization's branding
- Assigns the specified role (Reader, Editor, Admin) to users upon signup
- Handles both SSO and native authentication scenarios seamlessly
- Tracks invitation status and provides management capabilities

### Sending Email Invitations

To invite users via email, you need the `Manage User Credentials` [Platform Privilege](../../authorization/access-policies-guide.md).

1. Navigate to **Settings** > **Users & Groups**
2. Click the **Invite Users** button to open the invitation modal

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/invite-users-cta.png"/>
</p>

3. Switch to the **Invite via Email** tab
4. Enter one or more email addresses (separated by commas, spaces, or new lines)
5. Select the appropriate role for the invited users
6. Click **Invite**

### Email Invitation Features

- **Bulk Invitations**: Add multiple email addresses at once, making inviting larger groups of users a breeze
- **Input Validation**: Real-time validation prevents sending to invalid email addresses
- **Role Assignment**: Assign Reader, Editor, or Admin roles to invited users
- **SSO Integration**: Automatic handling when Single Sign-On is configured

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/invite-users-email-validation.png"/>
</p>

### Managing Pending Invitations

Invited users appear in your Users & Groups table with an "Invited" status. From the table, you can:

- **Resend Invitation**: Send the invitation email again to users who may not have received it
- **Delete Invitation**: Cancel the invitation and remove access for that email address
- **View Details**: See invitation status, assigned role, and invitation date

## User Invite Recommendations (DataHub Cloud)

DataHub Cloud analyzes usage data from your ingested platforms to automatically identify and recommend power users for invitation, making it easy to onboard your organization's most active data practitioners.

### How Recommendations Work

DataHub's recommendation engine:

- Analyzes usage patterns from your connected data platforms like Snowflake, BigQuery, Redshift, and more
- Identifies users with high query volumes and frequent data access
- Calculates usage percentiles to surface top performers
- Filters out users already invited or existing in your DataHub instance

For more details on how usage data is collected and analyzed, see the [Dataset Usage & Query History](../../features/dataset-usage-and-query-history.md) guide.

### Viewing Recommendations in the Invite Modal

Access top recommendations when inviting users:

1. Navigate to **Settings** > **Users & Groups**
2. Click **Invite Users**
3. Switch to the **Recommendations** tab
4. View the recommended users with highest query volume and invite with 1 click

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/invite-users-role-dropdown.png"/>
</p>

### Viewing All Recommendations

View all recommendations in the Users page:

1. Navigate to **Settings** > **Users & Groups**
2. Look for the "Recommended" section that lists all recent suggestions

### Recommendation Features

- **Platform-Specific Insights**: See which data platform each user is most active on
- **Usage Metrics**: View query counts, data access frequency, and usage percentiles
- **Smart Filtering**: Automatically excludes already invited or existing users
- **Fresh Data**: Recommendations update based on the latest usage analytics

### Inviting Recommended Users

From any view, you can:

- **Invite Individual Users**: Click the invite button next to any recommended user
- **Customize Roles**: Assign different roles based on user expertise and needs

## Shared Invite Links

Shared invite links provide a scalable way to onboard multiple users by generating a shareable URL with a pre-assigned role.

### Generating Invite Links

If you have the `Manage User Credentials` [Platform Privilege](../../authorization/access-policies-guide.md), you can create invite links:

1. Navigate to **Settings** > **Users & Groups**
2. Click the **Invite Users** button
3. In the **Invite Link** tab, select the role you want to assign to users who join via this link
4. Copy the generated link to share with your team

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/invite-users-popup.png"/>
</p>

### How Invite Links Work

When users visit your invite link:

- They're directed to a signup screen where they can create their DataHub account
- Their account is automatically assigned the role associated with the link
- If SSO is configured, they'll authenticate through your organization's identity provider
- If using native authentication, they'll create a username and password

### Invite Link Best Practices

- **Use role-specific links**: Create separate links for different permission levels (Reader, Editor, Admin)
- **Monitor usage**: Track who joins via invite links to ensure appropriate access
- **Refresh periodically**: Generate new links regularly for security purposes
- **Share securely**: Distribute links through secure channels to maintain access control

## Managing User Access

Once users are added to DataHub, administrators can manage their access and permissions:

### Resetting User Passwords

For users with native DataHub authentication, you can reset passwords:

1. Navigate to **Users & Groups**
2. Find the user and click the menu dropdown
3. Select **Reset user password**
4. Share the password reset link with the user (expires in 24 hours)

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/reset-user-password-popup.png"/>
</p>

### Modifying User Roles

Users with the `Manage User Credentials` privilege can update role assignments through the Users & Groups interface or via DataHub's APIs.

## Next Steps

Now that you understand DataHub's user invitation capabilities:

1. **Start with your core team**: Begin by inviting key stakeholders and power users who can help drive adoption
2. **Leverage recommendations**: Use DataHub's analytics to discover and invite active data practitioners from your platforms
3. **Scale gradually**: Use shared invite links for broader team onboarding once your core group is established
4. **Learn about policies**: For teams wanting to implement more granular role-based access control, explore [DataHub Policies](../../authorization/policies.md) to create fine-tuned permissions for different user groups

DataHub's flexible user management system grows with your needs, providing both targeted invitation capabilities for strategic onboarding and scalable methods for organization-wide adoption.
