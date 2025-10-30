# Business Glossary

import TutorialProgress from '@site/src/components/TutorialProgress';

<TutorialProgress
tutorialId="governance"
currentStep={2}
steps={[
{ title: 'Ownership Management', time: '12 min', description: 'Establish clear data ownership and accountability' },
{ title: 'Data Classification', time: '15 min', description: 'Implement PII detection and sensitivity labeling' },
{ title: 'Business Glossary', time: '12 min', description: 'Create standardized business terminology' },
{ title: 'Governance Policies', time: '11 min', description: 'Automate governance enforcement at scale' }
]}
/>

## Creating Consistent Business Language

**Time Required**: 12 minutes

### The Business Language Challenge

Your organization uses terms like "customer," "revenue," and "conversion" across different teams, but everyone has slightly different definitions. The marketing team's "active user" differs from the product team's definition, leading to:

- **Conflicting reports** with different numbers for the same metric
- **Wasted time** in meetings clarifying what terms mean
- **Poor decision-making** based on misunderstood data
- **Reduced trust** in data and analytics

**Real-World Impact**: Your executive team received two different "monthly revenue" reports with a $2M discrepancy because Finance and Sales defined "recognized revenue" differently.

### Understanding Business Glossaries

A business glossary provides standardized definitions for business terms, ensuring everyone speaks the same data language:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_lifetime_value"
    type="Table"
    platform="Snowflake"
    description="Calculated CLV using the standardized customer value methodology"
    owners={[
      { name: 'finance.team@company.com', type: 'Business Owner' }
    ]}
    tags={['Financial', 'KPI', 'Customer-Metrics']}
    glossaryTerms={['Customer Lifetime Value', 'Revenue Metric', 'Customer Value']}
    assertions={{ passing: 18, failing: 1, total: 19 }}
    health="Good"
  />
</div>

**Glossary Benefits**:

- **Consistent Definitions**: Single source of truth for business terms
- **Improved Communication**: Teams use standardized language
- **Better Data Discovery**: Find data using business terminology
- **Regulatory Compliance**: Clear definitions for audit requirements

### Exercise 1: Create Core Business Terms

Start by defining your organization's most important business concepts:

#### Step 1: Access the Glossary

1. **Navigate to "Glossary"** in DataHub's main menu
2. **Click "Create Term"** to add your first business term
3. **Review existing terms** to avoid duplicates

#### Step 2: Define "Active Customer"

Create a standardized definition for one of your most important terms:

1. **Term Name**: "Active Customer"
2. **Definition**: "A customer who has made at least one purchase or engaged with our platform within the last 90 days"
3. **Business Context**: "Used across Marketing, Product, and Finance teams for consistent customer reporting"
4. **Calculation Logic**: "WHERE last_activity_date >= CURRENT_DATE - 90"
5. **Related Terms**: Link to "Customer," "Engagement," "Retention"
6. **Owner**: Assign to your Customer Analytics team

#### Step 3: Add Financial Terms

Create definitions for key financial metrics:

**Revenue Recognition**:

- **Definition**: "Revenue recorded when goods are delivered or services are performed, following GAAP standards"
- **Business Rules**: "Subscription revenue recognized monthly; one-time purchases at delivery"
- **Calculation**: "SUM(recognized_amount) WHERE recognition_date <= report_date"

**Customer Lifetime Value (CLV)**:

- **Definition**: "Predicted total revenue from a customer over their entire relationship with the company"
- **Formula**: "Average Order Value × Purchase Frequency × Customer Lifespan"
- **Usage**: "Used for customer acquisition cost analysis and marketing budget allocation"

### Exercise 2: Link Terms to Datasets

Connect your business terms to actual data assets:

#### Step 1: Navigate to Dataset

1. **Open the customer analytics dataset** (e.g., "fct_users_created")
2. **Go to the "Properties" tab**
3. **Find the "Glossary Terms" section**

#### Step 2: Add Relevant Terms

1. **Click "Add Terms"**
2. **Search for "Active Customer"** and select it
3. **Add "Customer Lifetime Value"** if the dataset contains CLV calculations
4. **Add "Revenue Metric"** for any revenue-related fields
5. **Save the associations**

#### Step 3: Column-Level Term Assignment

For specific columns, add more granular terms:

- `customer_id` column → "Customer Identifier"
- `registration_date` column → "Customer Acquisition Date"
- `last_login_date` column → "Customer Activity Date"
- `total_spent` column → "Customer Value"

### Exercise 3: Create Term Hierarchies

Organize terms into logical hierarchies for better navigation:

#### Step 1: Create Term Categories

Set up high-level categories using DataHub's glossary hierarchy:

**Business Glossary Term Hierarchy:**

```
Customer Terms
├── 📂 Customer Identification
│   ├── Customer ID
│   └── Customer Segment
├── 📂 Customer Behavior
│   ├── Active Customer
│   └── Customer Engagement
└── 📂 Customer Value
    ├── Customer Lifetime Value (CLV)
    └── Customer Acquisition Cost (CAC)

Financial Terms
├── 📂 Revenue Metrics
│   ├── Revenue Recognition
│   └── Monthly Recurring Revenue (MRR)
└── 📂 Cost Metrics
    ├── Cost of Goods Sold (COGS)
    └── Operating Expenses (OPEX)
```

#### Step 2: Implement Hierarchies

1. **Create parent terms** for each category
2. **Link child terms** to their parents
3. **Add cross-references** between related terms
4. **Document relationships** in term descriptions

### Exercise 4: Establish Glossary Governance

Set up processes to maintain glossary quality:

#### Step 1: Assign Term Stewards

1. **For each business domain**, assign term stewards:

   - Customer terms → Customer Success Manager
   - Financial terms → Finance Business Analyst
   - Product terms → Product Manager
   - Marketing terms → Marketing Operations

2. **Define steward responsibilities**:
   - Review and approve new terms
   - Update definitions when business rules change
   - Resolve conflicts between similar terms

#### Step 2: Create Review Processes

1. **Quarterly term reviews**:

   - Verify definitions are still accurate
   - Update terms based on business changes
   - Archive obsolete terms

2. **New term approval workflow**:
   - Propose new terms through formal process
   - Business stakeholder review and approval
   - Technical validation of term usage

### Understanding Glossary Impact

A well-maintained business glossary delivers:

**Improved Data Literacy**:

- Business users understand data meaning
- Reduced time spent clarifying definitions
- Increased confidence in data-driven decisions

**Better Collaboration**:

- Consistent language across teams
- Faster onboarding of new team members
- More productive data discussions

**Enhanced Data Discovery**:

- Find datasets using business terminology
- Understand data context without technical expertise
- Discover related data through term relationships

### Advanced Glossary Features

#### 1. Term Lineage

Track how business terms relate to data lineage:

- See which datasets contribute to a business metric
- Understand impact of data changes on business terms
- Trace business definitions to source systems

#### 2. Automated Term Detection

Use DataHub's capabilities to:

- Automatically suggest terms for new datasets
- Detect when datasets match existing term definitions
- Alert when term usage becomes inconsistent

#### 3. Integration with BI Tools

Connect your glossary to:

- Business intelligence dashboards
- Reporting tools
- Data visualization platforms

### Measuring Glossary Success

Track these metrics to measure glossary adoption:

- **Term Coverage**: Percentage of datasets with glossary terms
- **Term Usage**: How often terms are referenced
- **Definition Consistency**: Alignment across different uses
- **User Engagement**: Active glossary users and contributions
- **Business Impact**: Reduction in definition-related confusion

### Best Practices for Business Glossaries

#### 1. Start with High-Impact Terms

Focus on terms that:

- Appear in executive reports
- Are used across multiple teams
- Have caused confusion in the past
- Are required for compliance

#### 2. Keep Definitions Business-Focused

- Use language business users understand
- Avoid technical jargon
- Include business context and usage
- Provide concrete examples

#### 3. Maintain Glossary Quality

- Regular reviews and updates
- Clear ownership and stewardship
- Version control for definition changes
- Feedback mechanisms for users

#### 4. Promote Adoption

- Training sessions for business users
- Integration with existing workflows
- Success stories and use cases
- Executive sponsorship and support

### Next Steps

With a comprehensive business glossary in place, you're ready to implement automated governance policies that enforce your data standards at scale.

<NextStepButton href="./governance-policies" />
