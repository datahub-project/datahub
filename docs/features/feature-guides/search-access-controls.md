import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Search Access Controls

<FeatureAvailability saasOnly />

Search Access Controls allow organizations to restrict which entities users can discover through search results. This feature uses the **View Entity** permission to filter search results based on policies, ensuring users only see metadata they are authorized to access.

## Key Concepts

### View Entity Permission

The **View Entity** permission controls whether a user can discover and access an entity. When Search Access Controls are enabled:

- Users will only see entities in search results that they have been granted access to view
- Users cannot access restricted entities via direct URL
- Access control applies consistently across search, browse, and direct access

This unified approach ensures that access control is applied consistently regardless of how a user attempts to discover or view an entity.

### Default Behavior

Without Search Access Controls enabled:

- All users can see all entities in search results
- No filtering is applied based on policies

When Search Access Controls are enabled:

- Search results are filtered based on the user's applicable policies
- Only entities matching at least one policy with the "View Entity" privilege are returned
- This creates a "default deny" model where explicit permission grants are required

### Policy-Based Filtering

Search results are automatically filtered based on:

- Active policies that grant the "View Entity" privilege

## How It Works

When a user performs a search:

1. DataHub identifies all active policies that grant the "View Entity" privilege to the user
2. For each policy, the resource filters are evaluated (domains, tags)
3. Only entities matching at least one applicable policy are included in the search results
4. The filtering happens at query time, ensuring consistent results across all search interfaces

The feature is enabled by your DataHub Cloud administrator. Contact your admin to enable Search Access Controls for your organization.

## Peer Group Recommendations

When Search Access Controls are enabled, the "Most Popular" recommendations on the home page can also be filtered to prevent information leakage.

### How Peer Groups Work

The peer group setting controls how "Most Popular" recommendations are calculated:

| Setting                 | Behavior                                                                                                                                                                                       |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Peer Group Enabled**  | Recommendations show what users in your same groups have been viewing. This allows you to see popular assets among your peers while preventing visibility into what other teams are accessing. |
| **Peer Group Disabled** | Recommendations are based only on your own activity. You will only see assets you have previously viewed.                                                                                      |

This prevents scenarios where users could infer the existence of sensitive data by seeing it appear in "Most Popular" recommendations, even if they cannot access it directly.

## Practical Scenario: Two Groups, Three Domains

This section walks through a complete example of setting up Search Access Controls for an organization with two teams that need different levels of access.

### Business Context

An organization wants to ensure:

- Engineering can only discover engineering-related data
- Finance can only discover financial data
- Both teams need access to shared company metrics

### Access Matrix

| Domain           | Engineering Team | Finance Team |
| ---------------- | ---------------- | ------------ |
| Engineering Data | Can View         | Cannot View  |
| Finance Data     | Cannot View      | Can View     |
| Company Metrics  | Can View         | Can View     |

### Step 1: Create the Domains

Navigate to **Settings > Domains** and create the following domains:

<p align="center">
  <img width="80%" style={{border: '1px solid #333', borderRadius: '6px'}} src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Domains.png"/>
</p>

1. **Engineering Data**

   - Description: Contains all engineering datasets, dashboards, and pipelines
   - Assign all engineering-related assets to this domain

2. **Finance Data**

   - Description: Contains all financial datasets and reports
   - Assign all finance-related assets to this domain

3. **Company Metrics**
   - Description: Contains shared KPIs and dashboards accessible to all teams
   - Assign cross-functional assets to this domain

<table style={{width: '100%', borderCollapse: 'separate', borderSpacing: '10px'}}>
  <tr>
    <td style={{width: '33%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Domain%20Engineering.png"/>
    </td>
    <td style={{width: '33%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Domain%20Finance.png"/>
    </td>
    <td style={{width: '33%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Domain%20Company%20Metrics.png"/>
    </td>
  </tr>
</table>

### Step 2: Create the Groups

Navigate to **Settings > Users & Groups > Groups** and create:

1. **Engineering Team**
   - Add all engineering users as members
2. **Finance Team**
   - Add all finance users as members

<table style={{width: '100%', borderCollapse: 'separate', borderSpacing: '10px'}}>
  <tr>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Groups.png"/>
    </td>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Users.png"/>
    </td>
  </tr>
</table>

### Step 3: Remove Default Read Access Policies

To ensure users can only see what they are explicitly granted access to, you must disable or remove the default read access policies:

1. Navigate to **Settings > Permissions > Policies**
2. Locate the default policies that grant "View Entity" to "All Users"
3. Disable or delete these policies

<p align="center">
  <img width="80%" style={{border: '1px solid #333', borderRadius: '6px'}} src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Deactivate%20Default%20Policies.png"/>
</p>

:::caution
Removing the default read access policies means users will not see any entities in search results until you create explicit access policies. Plan your access policies before making this change.
:::

### Step 4: Create Engineering Access Policy

Navigate to **Settings > Permissions > Policies** and click **Create Policy**:

1. **Policy Name**: Engineering Team - View Access
2. **Policy Type**: Metadata Policy
3. **Privileges**: Select **View Entity**
4. **Actors**:
   - Select the **Engineering Team** group
5. **Resources**:
   - From the Domain list, select **Engineering Data** and **Company Metrics**
6. Click **Save**

### Step 5: Create Finance Access Policy

Create another policy with:

1. **Policy Name**: Finance Team - View Access
2. **Policy Type**: Metadata Policy
3. **Privileges**: Select **View Entity**
4. **Actors**:
   - Select the **Finance Team** group
5. **Resources**:
   - From the Domain list, select **Finance Data** and **Company Metrics**
6. Click **Save**

<table style={{width: '100%', borderCollapse: 'separate', borderSpacing: '10px'}}>
  <tr>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Engineering%20Policy.png"/>
    </td>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Finance%20Policy.png"/>
    </td>
  </tr>
</table>

After creating both policies, you can view all configured view access policies:

<p align="center">
  <img width="80%" style={{border: '1px solid #333', borderRadius: '6px'}} src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/View%20Policies.png"/>
</p>

### Step 6: Verify the Configuration

Now let's verify that the access controls are working correctly by logging in as users from each group.

**Alice (Engineering Team)**

When Alice logs in and searches, she can only discover entities in the Engineering Data and Company Metrics domains:

<table style={{width: '100%', borderCollapse: 'separate', borderSpacing: '10px'}}>
  <tr>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Alice%20Login.png"/>
    </td>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/Alice%20Discover.png"/>
    </td>
  </tr>
</table>

**David (Finance Team)**

When David logs in and searches, he can only discover entities in the Finance Data and Company Metrics domains:

<table style={{width: '100%', borderCollapse: 'separate', borderSpacing: '10px'}}>
  <tr>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/David%20Login.png"/>
    </td>
    <td style={{width: '50%', verticalAlign: 'top', border: '1px solid #333', borderRadius: '6px', padding: 0}}>
      <img style={{display: 'block'}} width="100%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/search-access-controls/David%20Discover.png"/>
    </td>
  </tr>
</table>

## Important Considerations

### Domain Assignment

- **All entities should be assigned to domains** for this approach to work effectively
- Entities without a domain assignment will not be matched by domain-based policies
- Consider creating a catch-all policy or default domain for unassigned entities

### Platform Administrators

- Users with platform administrator privileges bypass Search Access Controls
- Admin users will see all entities regardless of policies
- Use admin accounts only when necessary

### Roles

- All roles (Admin, Editor, Reader) override view-based access policies
- Users assigned any role will be able to see all entities regardless of domain-based View Entity policies
- When configuring Search Access Controls, ensure users are not assigned any role if you want to restrict their access

### Consistent Access Control

- The View Entity permission applies to both search results and direct URL access
- Users without permission cannot discover entities in search or access them via direct link
- This ensures consistent access control regardless of how users attempt to view entities

### Resource Filter Types

Policies can filter resources by:

- **Domain**: Most common for organizational boundaries
- **Tag**: Useful for classification-based access (e.g., PII, Confidential) or granting access to specific entities

## FAQ

**What happens if an entity has no domain assigned?**

Entities without a domain will not match domain-based policies. These entities will only be visible to users with policies that:

- Grant access to all resources (no resource filter)
- Use other filter types (tags) that match the entity

**How do I grant access to specific entities rather than domains?**

Use tags to identify specific entities that should be accessible. Create a tag (e.g., "Finance Approved") and apply it to the entities you want to grant access to. Then create a policy with a tag-based resource filter. This approach is more maintainable than URN-based policies since you can easily add or remove entities by updating tags.

**Can I use tags instead of domains for access control?**

Yes. Instead of domain filters, select "Tag" as the resource filter type. This is useful when your access boundaries align with data classification rather than organizational structure.

**How do I troubleshoot when a user cannot see expected results?**

1. Verify the user is a member of the correct group
2. Check that the policy is active (not disabled)
3. Confirm the entity is assigned to the correct domain/has the correct tags
4. Verify the policy includes the "View Entity" privilege
5. Check that no conflicting deny policies exist
6. Remember that platform admins see all entities regardless of policies

**Do Search Access Controls affect the GraphQL API?**

Yes. The same filtering applies to programmatic access via the GraphQL API. Users will only receive entities they have permission to view.

**Can I create a policy that denies access instead of granting it?**

DataHub policies are grant-based. To deny access, you must remove the grant. Note that you also need to disable or remove the default read access policies that grant "View Entity" to all users (see Step 3 above). Once the default policies are removed and Search Access Controls are enabled, users have no access until explicitly granted.
