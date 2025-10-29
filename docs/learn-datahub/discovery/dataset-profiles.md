import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DataHubEntityCard from '@site/src/components/DataHubEntityCard';

# Understanding Dataset Profiles (20 minutes)

:::info Tutorial Progress
**Step 2 of 3** | **20 minutes** | [Overview](overview.md) → [Advanced Search](advanced-search.md) → **Dataset Profiles** → [Collaborative Discovery](collaborative-discovery.md)
:::

Learn to quickly assess data quality, understand schemas, and make informed decisions about whether a dataset meets your analysis needs. Transform from guessing to knowing.

## Discovery Challenge #2: The Data Detective

**Your Mission**: The customer dashboard shows suspicious numbers - customer count dropped 50% overnight. You need to evaluate potential data sources to find the root cause.

**What You'll Learn**: How to rapidly assess data quality, interpret statistics, and identify data issues using DataHub's automated profiling.

## The Dataset Intelligence Framework

Every dataset tells a story through its metadata. Learn to read these signals:

**Dataset Profile Analysis Workflow:**

1. **Dataset Discovery**: Locate potential datasets through search or browsing
2. **Quick Health Check**: Review freshness, completeness, and quality indicators
3. **Schema Analysis**: Examine column types, constraints, and relationships
4. **Quality Assessment**: Evaluate data distributions, null rates, and anomalies
5. **Usage Validation**: Check access patterns, downstream dependencies, and documentation
6. **Decision**: Determine dataset suitability for your use case

**Profile Reading Checklist:**

```
✓ Last Updated: Within acceptable freshness window?
✓ Row Count: Reasonable size for expected data volume?
✓ Column Quality: Acceptable null rates and distributions?
✓ Schema Stability: Consistent structure over time?
✓ Documentation: Sufficient context and business meaning?
✓ Access Patterns: Evidence of active usage by others?
```

## Quick Health Check (2 minutes)

Before diving deep, get a rapid overview of dataset health:

### The 30-Second Assessment

<div className="health-check-guide">

**Traffic Light System:**

| Green Light        | Yellow Light         | Red Light            |
| ------------------ | -------------------- | -------------------- |
| Updated < 24h ago  | Updated 1-7 days ago | Updated > 7 days ago |
| Has owner assigned | Owner unclear        | No owner             |
| Has description    | Minimal description  | No description       |
| Normal row count   | Row count changed    | Dramatic row changes |

</div>

**Visual Health Assessment Examples:**

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_analytics"
    type="Table"
    platform="Snowflake"
    description="Well-maintained customer analytics table with comprehensive metadata and active ownership"
    owners={[
      { name: 'Analytics Team', type: 'Business Owner' },
      { name: 'Data Engineering', type: 'Technical Owner' }
    ]}
    tags={['Production', 'Analytics', 'Customer-Data']}
    glossaryTerms={['Customer Analytics', 'Business Intelligence']}
    health="Good"
  />
  
  <DataHubEntityCard 
    name="temp_customer_backup"
    type="Table"
    platform="MySQL"
    description="Temporary backup table - use with caution"
    owners={[]}
    tags={['Temporary', 'Backup']}
    glossaryTerms={[]}
    health="Warning"
  />
  
  <DataHubEntityCard 
    name="legacy_customer_data"
    type="Table"
    platform="Oracle"
    description=""
    owners={[]}
    tags={['Legacy']}
    glossaryTerms={[]}
    health="Critical"
  />
</div>

### Interactive Exercise: Health Check Practice

<div className="interactive-exercise">

**Find and evaluate 3 customer-related datasets:**

1. **Open any customer dataset** from your previous search
2. **Look at the header area** - note the key indicators
3. **Fill out this assessment:**

```
Dataset Name: ________________
Last Updated: ________________
Owner: ______________________
Row Count: ___________________
Health Score: Good / Warning / Critical (circle one)
```

**Repeat for 2 more datasets and compare results**

</div>

## Schema Deep Dive (8 minutes)

The schema tells you what data is actually available and how it's structured:

### Reading the Schema Tab

<Tabs>
<TabItem value="column-analysis" label="Column Analysis">

**What to look for in each column:**

<div className="column-guide">

**Column Name Patterns:**

- `id`, `uuid`, `key` → Identifiers (good for joins)
- `created_at`, `updated_at` → Timestamps (good for time analysis)
- `email`, `phone`, `address` → PII (privacy considerations)
- `status`, `type`, `category` → Categorical data (good for grouping)
- `amount`, `count`, `score` → Numeric data (good for calculations)

**Data Type Insights:**

- `VARCHAR(255)` → Text fields, check for standardization
- `TIMESTAMP` → Time-based analysis possible
- `INTEGER` → Counting and math operations
- `DECIMAL(10,2)` → Monetary values, precise calculations
- `BOOLEAN` → Binary flags and filters

</div>

</TabItem>
<TabItem value="relationships" label="Relationships">

**Understanding table relationships:**

<div className="relationship-guide">

**🔑 Primary Keys:**

- Usually named `id`, `uuid`, or `[table]_id`
- Unique identifier for each row
- Essential for joins and deduplication

**Foreign Keys:**

- References to other tables
- Shows data relationships
- Enables cross-table analysis

**Composite Keys:**

- Multiple columns forming unique identifier
- Common in fact tables and junction tables
- Important for grain understanding

</div>

**Try This:** Look at a customer table schema and identify:

- Primary key column
- Foreign key relationships
- PII columns that need special handling
- Timestamp columns for temporal analysis

</TabItem>
<TabItem value="quality-indicators" label="Quality Indicators">

**Schema-level quality signals:**

<div className="quality-signals">

**High Quality Indicators:**

- Consistent naming conventions
- Comprehensive column descriptions
- Appropriate data types
- Clear primary/foreign key relationships
- Reasonable column count (not too sparse/dense)

**Quality Concerns:**

- Inconsistent naming (camelCase + snake_case)
- Missing column descriptions
- Generic column names (`col1`, `field_a`)
- All VARCHAR types (suggests poor modeling)
- Excessive NULL values in key columns

</div>

</TabItem>
</Tabs>

### Interactive Exercise: Schema Detective Work

<div className="interactive-exercise">

**Scenario**: You need to join customer data with order data for analysis.

**Your Task:**

1. **Find a customer dataset** and examine its schema
2. **Find an orders dataset** and examine its schema
3. **Identify the join key(s)** - what columns connect these tables?
4. **Assess join feasibility:**
   - Are the key columns the same data type?
   - Do the column names suggest they're related?
   - Are there any data quality concerns?

**Success Criteria:**

- Identified clear join path between tables
- Assessed potential data quality issues
- Understand what analysis would be possible

</div>

## Data Statistics & Profiling (7 minutes)

DataHub's automated profiling reveals data patterns and quality issues:

### Understanding Profile Statistics

<Tabs>
<TabItem value="numeric-stats" label="🔢 Numeric Columns">

**Key statistics to interpret:**

<div className="stats-guide">

| Statistic              | What It Tells You       | Red Flags                         |
| ---------------------- | ----------------------- | --------------------------------- |
| **Min/Max**            | Data range and outliers | Impossible values (negative ages) |
| **Mean/Median**        | Central tendency        | Large difference = skewed data    |
| **Null Count**         | Data completeness       | High nulls in key fields          |
| **Distinct Count**     | Data variety            | Too few = poor granularity        |
| **Standard Deviation** | Data spread             | Very high = inconsistent data     |

**Practice Interpretation:**

```
customer_age: Min=18, Max=150, Mean=45, Median=42, Nulls=5%
```

**Analysis**: Reasonable age range, slight right skew (mean > median), good completeness

</div>

</TabItem>
<TabItem value="categorical-stats" label="Categorical Columns">

**Understanding categorical data:**

<div className="categorical-guide">

**Value Distribution:**

- **Top Values**: Most common categories
- **Unique Count**: How many distinct values
- **Null Percentage**: Missing data rate

**Quality Signals:**

- **Good**: Clear categories, low null rate
- **Concerning**: Too many unique values, high null rate
- **Bad**: Inconsistent formatting, obvious data entry errors

**Example Analysis:**

```
customer_status:
- Active: 85% (good - most customers active)
- Inactive: 12% (reasonable churn)
- Pending: 3% (small processing queue)
- Nulls: 0% (excellent - no missing status)
```

</div>

</TabItem>
<TabItem value="temporal-stats" label="📅 Temporal Columns">

**Time-based data insights:**

<div className="temporal-guide">

**Temporal Patterns:**

- **Date Range**: How far back does data go?
- **Update Frequency**: Daily, hourly, real-time?
- **Gaps**: Missing time periods?
- **Seasonality**: Regular patterns?

**Business Relevance:**

- **Recent Data**: Good for current analysis
- **Historical Depth**: Enables trend analysis
- **Regular Updates**: Reliable for ongoing monitoring
- **Complete Coverage**: No missing business periods

</div>

</TabItem>
</Tabs>

### Interactive Exercise: Data Quality Detective

<div className="interactive-exercise">

**Mystery**: Customer count dropped 50% overnight. Use profiling data to investigate.

**Investigation Steps:**

1. **Find customer datasets** updated in the last 2 days
2. **Check row count trends** - look for dramatic changes
3. **Examine key columns** for anomalies:
   - Are there unusual null rates?
   - Do value distributions look normal?
   - Are there data type inconsistencies?

**Detective Questions:**

- Which dataset shows the row count drop?
- What columns might explain the change?
- Are there data quality issues that could cause undercounting?

**Report Your Findings:**

```
Suspect Dataset: ________________
Row Count Change: _______________
Potential Cause: ________________
Confidence Level: High/Medium/Low
```

</div>

## Usage Patterns & Validation (3 minutes)

Understand how others use this data to validate your choice:

### Query History Analysis

<div className="usage-guide">

**Usage Indicators:**

| Pattern               | Interpretation           | Decision Impact                 |
| --------------------- | ------------------------ | ------------------------------- |
| **High Query Volume** | Popular, trusted dataset | Good choice for analysis        |
| **Recent Queries**    | Actively used, current   | Likely up-to-date               |
| **Complex Queries**   | Rich analytical use      | Supports sophisticated analysis |
| **Simple Queries**    | Basic lookup use         | May lack analytical depth       |
| **No Recent Usage**   | Potentially stale        | Investigate before using        |

</div>

### User Feedback Signals

<Tabs>
<TabItem value="social-proof" label="👥 Social Proof">

**Look for community validation:**

- **Bookmarks/Follows**: How many users track this dataset?
- **Documentation Quality**: Well-documented = well-used
- **Owner Responsiveness**: Active owners = maintained data
- **Related Datasets**: Part of a larger, maintained ecosystem?

</TabItem>
<TabItem value="quality-feedback" label="Quality Feedback">

**User-generated quality signals:**

- **Tags**: `high-quality`, `production-ready`, `deprecated`
- **Comments**: User experiences and gotchas
- **Issues**: Known problems and limitations
- **Recommendations**: Alternative datasets for similar use cases

</TabItem>
</Tabs>

## Making the Go/No-Go Decision

Synthesize all information into a clear decision:

### Decision Framework

<div className="decision-matrix">

**Use This Dataset If:**

- Health check shows green/yellow lights
- Schema matches your analysis needs
- Data quality statistics look reasonable
- Usage patterns indicate active maintenance
- You can contact the owner if needed

**Investigate Further If:**

- Some quality concerns but dataset is unique
- Usage is low but data looks comprehensive
- Owner is unclear but data seems current

**Skip This Dataset If:**

- Multiple red flags in health check
- Schema doesn't support your use case
- Serious data quality issues
- No recent usage and no owner contact
- Better alternatives are available

</div>

### Final Exercise: Complete Dataset Evaluation

<div className="interactive-exercise">

**Challenge**: Evaluate 2 customer datasets and choose the better one for marketing analysis.

**Evaluation Scorecard:**

```
Dataset A: ________________    Dataset B: ________________

Health Check:     Excellent      Health Check:     Excellent
Schema Quality:   Excellent      Schema Quality:   Excellent
Data Quality:     Excellent      Data Quality:     Excellent
Usage Patterns:   Excellent      Usage Patterns:   Excellent
Total Score:      ___/20         Total Score:      ___/20

Winner: Dataset ___
Reason: ________________________
```

**Validation**: Can you justify your choice to a colleague in 30 seconds?

</div>

## Pro Tips for Efficient Evaluation

<div className="pro-tips">

**Speed Techniques:**

- Develop a mental checklist for rapid assessment
- Use browser tabs to compare multiple datasets
- Focus on deal-breakers first (freshness, schema fit)

**Accuracy Boosters:**

- Always check sample data when available
- Cross-reference with lineage to understand data flow
- Contact owners for clarification on edge cases

**Team Efficiency:**

- Document your evaluation criteria for consistency
- Share findings with teammates to avoid duplicate work
- Create team standards for "good enough" data quality

</div>

## Success Checkpoint

<div className="checkpoint">

**You've mastered dataset evaluation when you can:**

**Speed Test**: Complete health check + schema review in under 5 minutes
**Quality Test**: Identify 3 potential data quality issues from profiling stats
**Decision Test**: Make confident go/no-go decisions with clear justification
**Communication Test**: Explain dataset suitability to stakeholders

**Final Validation:**
Choose the best customer dataset for a marketing campaign analysis. Justify your choice in 3 bullet points covering health, schema, and quality.

</div>

## Common Evaluation Pitfalls

<Tabs>
<TabItem value="perfectionism" label="Perfectionism Trap">

**Problem**: Waiting for perfect data that doesn't exist

**Solution**:

- Define "good enough" criteria upfront
- Focus on fitness for purpose, not perfection
- Consider data improvement as part of your project

</TabItem>
<TabItem value="surface-level" label="🏊 Surface-Level Analysis">

**Problem**: Making decisions based only on names and descriptions

**Solution**:

- Always check actual schema and statistics
- Look at sample data when available
- Verify assumptions with data owners

</TabItem>
<TabItem value="isolation" label="Isolation Analysis">

**Problem**: Evaluating datasets in isolation without considering alternatives

**Solution**:

- Always compare 2-3 options when possible
- Consider combining multiple datasets
- Check lineage for upstream/downstream alternatives

</TabItem>
</Tabs>

## What You've Learned

**Excellent work!** You can now rapidly assess dataset quality and make informed decisions:

- **Health Assessment**: Quick evaluation of dataset reliability
- **Schema Intelligence**: Understanding structure and relationships
- **Quality Analysis**: Interpreting statistics and profiling data
- **Usage Validation**: Leveraging community knowledge
- **Decision Framework**: Systematic go/no-go evaluation

---

**Next**: Now that you can find and evaluate data, let's learn how to [collaborate and share knowledge](collaborative-discovery.md) with your team →
