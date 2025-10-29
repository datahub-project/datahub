import DataHubEntityCard from '@site/src/components/DataHubEntityCard';

# Data Assertions

<TutorialProgress
currentStep="data-assertions"
steps={[
{ id: 'quality-overview', label: 'Overview', completed: true },
{ id: 'data-assertions', label: 'Data Assertions', completed: false },
{ id: 'quality-monitoring', label: 'Quality Monitoring', completed: false },
{ id: 'incident-management', label: 'Incident Management', completed: false },
{ id: 'quality-automation', label: 'Quality Automation', completed: false }
]}
compact={true}
/>

## Building Automated Data Quality Checks

**Time Required**: 15 minutes

### The Assertion Challenge

Your data pipelines are processing customer transactions, but you're discovering quality issues after they've already impacted business operations:

- **Missing customer IDs** causing failed order processing
- **Negative transaction amounts** appearing in financial reports
- **Duplicate records** inflating customer metrics
- **Stale data** making real-time dashboards unreliable

**Real-World Impact**: Last week, a batch of transactions with null customer IDs caused the customer service system to crash, resulting in 4 hours of downtime and frustrated customers.

### Understanding DataHub Assertions

Assertions are automated quality checks that continuously validate your data against business rules:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_transactions"
    type="Table"
    platform="Snowflake"
    description="Customer transaction records with comprehensive quality validation"
    owners={[
      { name: 'payments.team@company.com', type: 'Technical Owner' }
    ]}
    tags={['Financial', 'Critical', 'Quality-Monitored']}
    glossaryTerms={['Transaction Record', 'Payment Data']}
    assertions={{ passing: 15, failing: 2, total: 17 }}
    health="Warning"
  />
</div>

**Assertion Types**:

- **Completeness**: Ensure required fields are not null or empty
- **Uniqueness**: Validate primary keys and unique constraints
- **Range Validation**: Check numeric values fall within expected bounds
- **Freshness**: Verify data is updated within acceptable time windows
- **Referential Integrity**: Ensure foreign key relationships are valid
- **Custom Rules**: Implement business-specific validation logic

### Exercise 1: Create Completeness Assertions

Ensure critical fields always contain valid data:

#### Step 1: Navigate to Assertions

1. **Open DataHub** and search for "fct_users_created"
2. **Click on the dataset** to open its profile page
3. **Go to the "Quality" tab** and click "Add Assertion"
4. **Select "Column Assertion"** to validate specific fields

#### Step 2: Create Customer ID Completeness Check

**Assertion Configuration**:

- **Name**: "Customer ID Required"
- **Description**: "All records must have a valid customer_id"
- **Column**: `customer_id`
- **Type**: "Not Null"
- **Severity**: "Error" (blocks downstream processing)
- **Schedule**: "Every 15 minutes"

**SQL Logic**:

```sql
SELECT COUNT(*) as null_count
FROM fct_users_created
WHERE customer_id IS NULL
  OR customer_id = ''
```

**Success Criteria**: `null_count = 0`

#### Step 3: Add Email Validation

**Assertion Configuration**:

- **Name**: "Valid Email Format"
- **Description**: "Email addresses must follow standard format"
- **Column**: `email`
- **Type**: "Custom SQL"
- **Severity**: "Warning"

**SQL Logic**:

```sql
SELECT COUNT(*) as invalid_emails
FROM fct_users_created
WHERE email IS NOT NULL
  AND email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
```

**Success Criteria**: `invalid_emails = 0`

### Exercise 2: Implement Range Validations

Validate that numeric values fall within business-acceptable ranges:

#### Step 1: Transaction Amount Validation

For financial data, create bounds checking:

**Assertion Configuration**:

- **Name**: "Valid Transaction Amount"
- **Description**: "Transaction amounts must be positive and reasonable"
- **Column**: `transaction_amount`
- **Type**: "Range Check"
- **Min Value**: 0.01 (no zero or negative transactions)
- **Max Value**: 100000.00 (flag unusually large transactions)
- **Severity**: "Error"

#### Step 2: Date Range Validation

Ensure dates are realistic and current:

**Assertion Configuration**:

- **Name**: "Recent Transaction Date"
- **Description**: "Transaction dates should be within the last 2 years"
- **Column**: `transaction_date`
- **Type**: "Custom SQL"
- **Severity**: "Warning"

**SQL Logic**:

```sql
SELECT COUNT(*) as invalid_dates
FROM customer_transactions
WHERE transaction_date < CURRENT_DATE - INTERVAL '2 years'
   OR transaction_date > CURRENT_DATE + INTERVAL '1 day'
```

### Exercise 3: Create Uniqueness Assertions

Prevent duplicate records that can skew analytics:

#### Step 1: Primary Key Uniqueness

**Assertion Configuration**:

- **Name**: "Unique Transaction ID"
- **Description**: "Each transaction must have a unique identifier"
- **Column**: `transaction_id`
- **Type**: "Uniqueness"
- **Severity**: "Error"
- **Action**: "Block pipeline on failure"

#### Step 2: Business Key Uniqueness

For composite business keys:

**Assertion Configuration**:

- **Name**: "Unique Customer-Date Combination"
- **Description**: "One transaction per customer per day (business rule)"
- **Type**: "Custom SQL"
- **Severity**: "Warning"

**SQL Logic**:

```sql
SELECT customer_id, DATE(transaction_date) as txn_date, COUNT(*) as duplicate_count
FROM customer_transactions
GROUP BY customer_id, DATE(transaction_date)
HAVING COUNT(*) > 1
```

### Exercise 4: Implement Freshness Checks

Ensure data is updated according to business requirements:

#### Step 1: Data Freshness Assertion

**Assertion Configuration**:

- **Name**: "Customer Data Freshness"
- **Description**: "Customer data must be updated within 4 hours"
- **Type**: "Freshness"
- **Column**: `last_updated_timestamp`
- **Max Age**: "4 hours"
- **Severity**: "Error"

#### Step 2: Partition Freshness

For partitioned tables:

**SQL Logic**:

```sql
SELECT MAX(partition_date) as latest_partition
FROM customer_transactions
WHERE partition_date >= CURRENT_DATE - INTERVAL '1 day'
```

**Success Criteria**: `latest_partition >= CURRENT_DATE`

### Exercise 5: Custom Business Rule Assertions

Implement organization-specific validation logic:

#### Step 1: Customer Lifecycle Validation

**Business Rule**: "Customers must have a registration date before their first transaction"

**Assertion Configuration**:

- **Name**: "Valid Customer Lifecycle"
- **Description**: "Registration must precede first transaction"
- **Type**: "Custom SQL"
- **Severity**: "Error"

**SQL Logic**:

```sql
SELECT COUNT(*) as lifecycle_violations
FROM customer_transactions ct
JOIN customer_profiles cp ON ct.customer_id = cp.customer_id
WHERE ct.transaction_date < cp.registration_date
```

#### Step 2: Revenue Recognition Rules

**Business Rule**: "Subscription revenue must be recognized monthly"

**SQL Logic**:

```sql
SELECT COUNT(*) as recognition_errors
FROM revenue_transactions
WHERE product_type = 'subscription'
  AND recognition_method != 'monthly'
```

### Understanding Assertion Results

DataHub provides comprehensive assertion monitoring:

**Assertion Status Indicators**:

- **Passing**: All validation rules met
- **Warning**: Minor issues detected, investigate soon
- **Failing**: Critical issues found, immediate attention required
- **Paused**: Assertion temporarily disabled
- 🔄 **Running**: Currently executing validation

**Assertion History**:

- Track assertion results over time
- Identify patterns in quality issues
- Measure quality improvement trends
- Generate compliance reports

### Best Practices for Data Assertions

#### 1. Start with Critical Business Rules

Focus on assertions that protect:

- Revenue calculations
- Customer data integrity
- Regulatory compliance requirements
- Downstream system dependencies

#### 2. Use Appropriate Severity Levels

- **Error**: Critical issues that must block processing
- **Warning**: Issues that need investigation but don't stop pipelines
- **Info**: Monitoring checks for trend analysis

#### 3. Optimize Assertion Performance

- Use efficient SQL queries
- Leverage table statistics when possible
- Schedule assertions based on data update frequency
- Consider sampling for large datasets

#### 4. Provide Clear Context

- Write descriptive assertion names and descriptions
- Document business rationale for each rule
- Include remediation guidance
- Link to relevant business stakeholders

### Measuring Assertion Effectiveness

Track these key metrics:

- **Assertion Coverage**: Percentage of critical columns with assertions
- **Pass Rate**: Percentage of assertions passing over time
- **Detection Speed**: Time from data issue to assertion failure
- **False Positive Rate**: Assertions failing due to rule issues
- **Business Impact Prevention**: Issues caught before affecting operations

### Advanced Assertion Techniques

#### 1. Statistical Assertions

Monitor data distributions and detect anomalies:

```sql
-- Detect unusual spikes in transaction volume
SELECT COUNT(*) as daily_transactions
FROM customer_transactions
WHERE DATE(transaction_date) = CURRENT_DATE
HAVING COUNT(*) > (
  SELECT AVG(daily_count) * 2
  FROM daily_transaction_stats
  WHERE date >= CURRENT_DATE - INTERVAL '30 days'
)
```

#### 2. Cross-Dataset Assertions

Validate consistency across related datasets:

```sql
-- Ensure customer counts match between systems
SELECT ABS(
  (SELECT COUNT(DISTINCT customer_id) FROM crm_customers) -
  (SELECT COUNT(DISTINCT customer_id) FROM billing_customers)
) as customer_count_diff
HAVING customer_count_diff <= 10  -- Allow small variance
```

#### 3. Time-Series Assertions

Monitor trends and seasonal patterns:

```sql
-- Detect unusual drops in daily active users
SELECT current_dau, previous_week_avg
FROM (
  SELECT
    COUNT(DISTINCT user_id) as current_dau,
    LAG(COUNT(DISTINCT user_id), 7) OVER (ORDER BY date) as previous_week_avg
  FROM user_activity
  WHERE date = CURRENT_DATE
)
WHERE current_dau < previous_week_avg * 0.8  -- 20% drop threshold
```

### Next Steps

With automated assertions in place, you're ready to build comprehensive quality monitoring dashboards that provide real-time visibility into your data health.

<NextStepButton href="./quality-monitoring.md" />
