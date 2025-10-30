# Data Classification

import TutorialProgress from '@site/src/components/TutorialProgress';

<TutorialProgress
tutorialId="governance"
currentStep={1}
steps={[
{ title: 'Ownership Management', time: '12 min', description: 'Establish clear data ownership and accountability' },
{ title: 'Data Classification', time: '15 min', description: 'Implement PII detection and sensitivity labeling' },
{ title: 'Business Glossary', time: '12 min', description: 'Create standardized business terminology' },
{ title: 'Governance Policies', time: '11 min', description: 'Automate governance enforcement at scale' }
]}
/>

## Protecting Sensitive Data Through Classification

**Time Required**: 15 minutes

### The Classification Challenge

Your company handles customer PII, financial data, and proprietary business information across hundreds of datasets. Without proper classification, you can't:

- **Comply with regulations** like GDPR, CCPA, or SOX
- **Implement appropriate security controls** for different data types
- **Respond to data subject requests** or audit requirements
- **Prevent accidental exposure** of sensitive information

**Real-World Scenario**: During a recent audit, your team couldn't quickly identify which datasets contained PII, leading to a 2-week manual review process and potential compliance penalties.

### Understanding Data Classification Levels

DataHub supports industry-standard classification levels:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_profiles"
    type="Table"
    platform="PostgreSQL"
    description="Customer personal information including names, emails, and addresses"
    owners={[
      { name: 'privacy.team@company.com', type: 'Data Steward' }
    ]}
    tags={['PII', 'Restricted', 'GDPR-Sensitive']}
    glossaryTerms={['Personal Data', 'Customer Information']}
    assertions={{ passing: 8, failing: 2, total: 10 }}
    health="Warning"
  />
  
  <DataHubEntityCard 
    name="product_analytics"
    type="Table"
    platform="BigQuery"
    description="Aggregated product usage metrics without personal identifiers"
    owners={[
      { name: 'product.team@company.com', type: 'Business Owner' }
    ]}
    tags={['Internal', 'Analytics', 'Non-PII']}
    glossaryTerms={['Product Metrics', 'Usage Data']}
    assertions={{ passing: 15, failing: 0, total: 15 }}
    health="Good"
  />
</div>

**Classification Levels**:

- **Restricted**: PII, financial data, trade secrets (highest protection)
- **Confidential**: Internal business data, customer insights
- **🔵 Internal**: General business information, operational data
- **Public**: Marketing materials, published reports

### Exercise 1: Implement PII Detection

Set up automated detection of personally identifiable information:

#### Step 1: Enable PII Classification

1. **Navigate to Settings** → **Classification**
2. **Enable "Automatic PII Detection"**
3. **Configure detection patterns** for:
   - Email addresses (`*email*`, `*e_mail*`)
   - Phone numbers (`*phone*`, `*mobile*`)
   - Social Security Numbers (`*ssn*`, `*social_security*`)
   - Credit card numbers (`*card*`, `*payment*`)

#### Step 2: Review Detected PII

1. **Go to the "Classification" dashboard**
2. **Review automatically detected PII fields**
3. **Verify accuracy** of the detection
4. **Manually classify** any missed sensitive fields

#### Step 3: Apply PII Tags

For the customer dataset:

1. **Open the dataset profile**
2. **Navigate to the Schema tab**
3. **For each PII column**, add appropriate tags:
   - `email` column → Add "PII" and "Contact-Info" tags
   - `phone` column → Add "PII" and "Contact-Info" tags
   - `address` column → Add "PII" and "Location-Data" tags

### Exercise 2: Set Up Classification Rules

Create automated rules to classify data based on patterns:

#### Create Classification Rules

1. **Go to Settings** → **Classification Rules**
2. **Create new rule**: "Financial Data Detection"

   - **Pattern**: Column names containing `*amount*`, `*price*`, `*cost*`, `*revenue*`
   - **Classification**: "Confidential"
   - **Tags**: "Financial", "Sensitive"

3. **Create new rule**: "Customer Data Detection"
   - **Pattern**: Table names containing `*customer*`, `*user*`, `*client*`
   - **Classification**: "Restricted"
   - **Tags**: "Customer-Data", "High-Privacy"

#### Test Classification Rules

1. **Run classification** on sample datasets
2. **Review results** in the Classification dashboard
3. **Adjust rules** based on accuracy
4. **Schedule regular re-classification** to catch new data

### Exercise 3: Implement Data Sensitivity Levels

Apply consistent sensitivity labeling across your data landscape:

#### Step 1: Define Sensitivity Framework

Create a company-wide sensitivity framework:

```
Sensitivity Level | Data Types | Access Controls | Examples
-----------------|------------|-----------------|----------
Restricted       | PII, PHI, Financial | Role-based, Encrypted | SSN, Credit Cards
Confidential     | Business Critical | Department-based | Revenue, Strategy
Internal         | Operational | Employee Access | Logs, Metrics
Public           | Marketing | Open Access | Press Releases
```

#### Step 2: Apply Sensitivity Labels

1. **Navigate to each critical dataset**
2. **Add sensitivity tags**:

   - Customer data → "Restricted"
   - Financial reports → "Confidential"
   - System logs → "Internal"
   - Marketing content → "Public"

3. **Document classification rationale** in dataset descriptions

### Exercise 4: Set Up Compliance Monitoring

Monitor classification compliance across your data landscape:

#### Create Compliance Dashboard

1. **Go to Analytics** → **Governance Metrics**
2. **Create dashboard** with these metrics:
   - Percentage of datasets classified
   - Number of PII fields identified
   - Compliance score by data domain
   - Classification coverage trends

#### Set Up Compliance Alerts

1. **Configure alerts** for:
   - New datasets without classification
   - PII detected in unclassified data
   - Changes to restricted data schemas
   - Access to sensitive data outside business hours

### Understanding Classification Impact

Proper data classification enables:

**Regulatory Compliance**:

- Quick identification of data subject to regulations
- Automated compliance reporting
- Audit trail for data handling

**Risk Management**:

- Appropriate security controls for different data types
- Incident response prioritization
- Data breach impact assessment

**Access Control**:

- Role-based access to sensitive data
- Automated access reviews
- Principle of least privilege enforcement

### Advanced Classification Techniques

#### 1. Machine Learning-Based Classification

Use DataHub's ML capabilities to:

- Analyze data content patterns
- Identify sensitive data in unstructured fields
- Continuously improve classification accuracy

#### 2. Column-Level Classification

Apply granular classification:

- Different sensitivity levels within the same table
- Field-specific access controls
- Detailed compliance mapping

#### 3. Dynamic Classification

Implement classification that adapts to:

- Data content changes
- Business context evolution
- Regulatory requirement updates

### Measuring Classification Success

Track these key metrics:

- **Classification Coverage**: Percentage of datasets classified
- **PII Detection Accuracy**: True positives vs false positives
- **Compliance Score**: Adherence to classification policies
- **Time to Classify**: Speed of classifying new datasets
- **Access Violations**: Unauthorized access to classified data

### Best Practices for Data Classification

#### 1. Start with High-Risk Data

Prioritize classification of:

- Customer PII
- Financial information
- Healthcare data
- Intellectual property

#### 2. Automate Where Possible

Use automated detection for:

- Common PII patterns
- Standard data types
- Regulatory data categories

#### 3. Regular Review and Updates

- Quarterly classification reviews
- Updates for new data sources
- Refinement of classification rules

#### 4. Training and Awareness

- Educate data teams on classification importance
- Provide clear classification guidelines
- Regular training on new regulations

### Next Steps

With data properly classified, you're ready to create a business glossary that provides consistent definitions and context for your data assets.

<NextStepButton href="./business-glossary" />
