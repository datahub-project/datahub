# Ownership Management

<TutorialProgress
tutorialId="governance"
currentStep="ownership-management"
steps={[
{ id: 'governance-overview', title: 'Overview', time: '5 min', description: 'Professional data governance journey introduction' },
{ id: 'ownership-management', title: 'Ownership Management', time: '12 min', description: 'Establish clear data ownership and accountability' },
{ id: 'data-classification', title: 'Data Classification', time: '15 min', description: 'Implement PII detection and sensitivity labeling' },
{ id: 'business-glossary', title: 'Business Glossary', time: '12 min', description: 'Create standardized business terminology' },
{ id: 'governance-policies', title: 'Governance Policies', time: '11 min', description: 'Automate governance enforcement at scale' }
]}
/>

## Establishing Clear Data Ownership

**Time Required**: 12 minutes

### The Ownership Challenge

Your organization has critical datasets like customer information, financial transactions, and product analytics, but when data quality issues arise, nobody knows who to contact. Teams waste hours trying to find the right person to fix problems or answer questions about data.

**Real-World Impact**: A recent customer complaint about incorrect billing took 3 days to resolve because the team couldn't identify who owned the billing data pipeline.

### Understanding DataHub Ownership Types

DataHub supports multiple ownership types to reflect real organizational structures:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_transactions"
    type="Table"
    platform="Snowflake"
    description="Customer transaction history with payment details and billing information"
    owners={[
      { name: 'sarah.johnson@company.com', type: 'Technical Owner' },
      { name: 'mike.chen@company.com', type: 'Business Owner' },
      { name: 'data.stewards@company.com', type: 'Data Steward' }
    ]}
    tags={['PII', 'Financial', 'Critical']}
    glossaryTerms={['Customer Data', 'Transaction Record']}
    assertions={{ passing: 12, failing: 0, total: 12 }}
    health="Good"
  />
</div>

**Ownership Types Explained**:

- **Technical Owner**: Responsible for data pipeline maintenance, schema changes, and technical issues
- **Business Owner**: Accountable for data accuracy, business rules, and stakeholder communication
- **Data Steward**: Ensures data quality, compliance, and governance standards
- **Data Owner**: Ultimate accountability for data asset (often a senior business leader)

### Exercise 1: Assign Dataset Owners

Let's establish ownership for your organization's key datasets:

#### Step 1: Navigate to Dataset Ownership

1. **Open DataHub** and search for "fct_users_created"
2. **Click on the dataset** to open its profile page
3. **Go to the "Properties" tab** and find the "Ownership" section
4. **Click "Add Owners"** to begin assignment

#### Step 2: Add Technical Owner

1. **Select "Technical Owner"** from the ownership type dropdown
2. **Enter the email**: `john.doe@company.com`
3. **Add justification**: "Maintains the user analytics ETL pipeline"
4. **Click "Add"** to save

#### Step 3: Add Business Owner

1. **Click "Add Owners"** again
2. **Select "Business Owner"**
3. **Enter the email**: `sarah.smith@company.com`
4. **Add justification**: "Accountable for user metrics accuracy and business requirements"
5. **Click "Add"** to save

### Exercise 2: Set Up Ownership Notifications

Configure automatic notifications so owners are alerted to important events:

#### Configure Owner Notifications

1. **Go to Settings** → **Notifications**
2. **Enable "Dataset Quality Alerts"** for Technical Owners
3. **Enable "Schema Change Notifications"** for Business Owners
4. **Set up "Data Incident Alerts"** for Data Stewards

**What This Achieves**: When data quality issues occur, the right people are automatically notified based on their ownership role.

### Exercise 3: Create Ownership Hierarchies

For large organizations, establish ownership hierarchies by domain:

#### Domain-Based Ownership Structure

**Customer Domain:**

- **Technical Owners**: Data Engineering Team (infrastructure, pipelines, technical maintenance)
- **Business Owners**: Customer Success Team (business requirements, use cases)
- **Data Stewards**: Customer Data Governance (quality, compliance, documentation)
- **Data Owner**: VP Customer Experience (strategic decisions, access approvals)

**Financial Domain:**

- **Technical Owners**: Financial Systems Team (ERP integration, data processing)
- **Business Owners**: Finance Team (reporting requirements, business rules)
- **Data Stewards**: Financial Data Governance (regulatory compliance, audit trails)
- **Data Owner**: CFO (strategic oversight, regulatory accountability)

#### Implement Domain Ownership

1. **Navigate to "Domains"** in DataHub
2. **Create "Customer Domain"** if it doesn't exist
3. **Add datasets** to the appropriate domain
4. **Assign domain-level owners** who oversee all datasets in that domain

### Understanding Ownership Impact

With proper ownership in place, your organization gains:

**Faster Issue Resolution**:

- Data quality problems get routed to the right technical owner
- Business questions go directly to the business owner
- Average resolution time drops from days to hours

**Clear Accountability**:

- Each dataset has designated responsible parties
- Ownership information is visible to all data consumers
- No more "orphaned" datasets without clear ownership

**Improved Data Quality**:

- Owners receive proactive alerts about their data
- Regular ownership reviews ensure assignments stay current
- Quality metrics are tied to specific owners

### Best Practices for Ownership Management

#### 1. Start with Critical Datasets

Focus on your most important data assets first:

- Customer data
- Financial transactions
- Product analytics
- Regulatory reporting data

#### 2. Use Multiple Ownership Types

Don't rely on just one owner per dataset:

- Technical owners for operational issues
- Business owners for accuracy and requirements
- Data stewards for governance and compliance

#### 3. Regular Ownership Reviews

Set up quarterly reviews to:

- Verify ownership assignments are current
- Update owners when people change roles
- Add ownership to newly discovered datasets

#### 4. Document Ownership Responsibilities

Create clear expectations for each ownership type:

- Response time commitments
- Escalation procedures
- Quality standards

### Measuring Ownership Success

Track these metrics to measure the impact of your ownership program:

- **Mean Time to Resolution (MTTR)** for data issues
- **Percentage of datasets with assigned owners**
- **Owner response rate** to data quality alerts
- **User satisfaction** with data issue resolution

### Next Steps

Now that you've established clear ownership, you're ready to implement data classification to identify and protect sensitive information.

<NextStepButton href="./data-classification.md" />
