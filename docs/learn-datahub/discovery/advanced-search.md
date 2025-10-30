import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';
import DataHubEntityCard from '@site/src/components/DataHubEntityCard';
import { SearchExercise, HandsOnExercise, InteractiveDemo } from '@site/src/components/TutorialExercise';
import NextStepButton from '@site/src/components/NextStepButton';
import TutorialProgress from '@site/src/components/TutorialProgress';

# Advanced Search Techniques (15 minutes)

<TutorialProgress
tutorialId="discovery"
currentStep={0}
steps={[
{ title: "Advanced Search Techniques", time: "15 min", description: "Master operators, filters, and saved searches" },
{ title: "Understanding Dataset Profiles", time: "20 min", description: "Interpret profiles, statistics, and data quality" },
{ title: "Collaborative Discovery", time: "10 min", description: "Document, tag, and share knowledge effectively" }
]}
/>

Master DataHub's powerful search capabilities to find exactly what you need, when you need it. Transform from basic keyword searching to surgical data discovery.

## Scenario 1: Targeted Data Discovery

**Objective**: Find customer segmentation data for a marketing campaign without exact table names or locations.

**What You'll Learn**: Strategic search approaches, advanced operators, and effective filtering techniques.

## Search Strategy Framework

Effective data discovery follows a systematic approach:

**Professional Search Strategy:**

1. **Start with Business Terms**: Use domain-specific language and common business concepts
2. **Apply Smart Filters**: Narrow scope using platform, domain, and entity type filters
3. **Refine with Operators**: Use advanced search operators for precise matching
4. **Validate Results**: Review relevance, quality, and completeness of results
5. **Save for Reuse**: Create saved searches for recurring discovery needs

**Search Progression Example:**

```
Initial Query: "customer data"
↓ Add Filters: Platform=Hive, Type=Dataset
↓ Use Operators: "customer" AND ("profile" OR "behavior")
↓ Validate: Check schema, lineage, and documentation
↓ Save: "Customer Analytics Datasets" search
```

Let's apply this framework to solve our challenge!

## Level 1: Strategic Keyword Search

Start with business concepts, not technical terms:

<Tabs>
<TabItem value="business-first" label="Business-First Approach">

**Try these searches in your DataHub instance:**

<SearchExercise
title="Business-First Search Strategy"
difficulty="beginner"
timeEstimate="3 min"
searches={[
{
query: "customer segmentation",
description: "Search using business terminology",
expected: "Tables with business-friendly names and descriptions"
},
{
query: "customer segment cohort marketing",
description: "Combine multiple business terms",
expected: "Analytics tables and marketing-specific datasets"
},
{
query: "customer analytics behavior analysis",
description: "Use analysis-focused keywords",
expected: "Analytical views and processed datasets"
}
]}
/>

**What You'll Find**: Here are examples of the datasets your search would discover:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_segments"
    type="Table"
    platform="Snowflake"
    description="Customer segmentation analysis with behavioral and demographic groupings"
    owners={[
      { name: 'Marketing Analytics', type: 'Business Owner' }
    ]}
    tags={['Customer-Analytics', 'Segmentation', 'Marketing']}
    glossaryTerms={['Customer Segment', 'Marketing Analytics']}
    health="Good"
  />
  
  <DataHubEntityCard 
    name="customer_cohort_analysis"
    type="View"
    platform="Hive"
    description="Monthly customer cohort retention and behavior analysis"
    owners={[
      { name: 'Data Science Team', type: 'Technical Owner' }
    ]}
    tags={['Cohort-Analysis', 'Retention', 'Analytics']}
    glossaryTerms={['Customer Cohort', 'Retention Analysis']}
    health="Good"
  />
</div>



:::tip Pro Tip
Business users often name things differently than technical teams. Try both perspectives!
:::

</TabItem>
<TabItem value="technical-terms" label="Technical Terms">

**When business terms don't work, try technical approaches:**

<div className="search-exercise">

**Search 1: Database Patterns**

```
customer_segment user_cohort cust_analytics
```

**Search 2: Common Prefixes**

```
dim_customer fact_customer customer_dim
```

**Search 3: Analytics Patterns**

```
customer_ltv customer_score customer_tier
```

</div>

</TabItem>
</Tabs>

### Interactive Exercise: Your First Search

<div className="interactive-exercise">

**Try this now in DataHub:**

1. **Open DataHub** at http://localhost:9002
2. **Search for**: `customer segmentation`
3. **Count the results**: How many datasets appear?
4. **Note the variety**: Different platforms, naming conventions, descriptions

**Reflection Questions:**

- Which results look most relevant for marketing analysis?
- What patterns do you notice in the naming conventions?
- Are there results you didn't expect?

</div>

## Level 2: Smart Filtering

Raw search results can be overwhelming. Use filters to focus on what matters:

### Platform Filtering

<Tabs>
<TabItem value="filter-demo" label="Filter Walkthrough">

**Follow along in DataHub:**

1. **Search**: `customer`
2. **Apply Platform Filter**:
   - Click "Filters" in the left sidebar
   - Select "Platform"
   - Choose "PostgreSQL" (for operational data)
   - OR choose "Snowflake" (for analytics data)

**Notice how results change!**

</TabItem>
<TabItem value="filter-strategy" label="Filter Strategy">

**Choose filters based on your use case:**

**For Marketing Analysis:**

- Snowflake, BigQuery (analytics platforms)
- dbt (transformed data)
- MySQL, PostgreSQL (raw operational data) - not recommended

**For Operational Insights:**

- PostgreSQL, MySQL (live operational data)
- Kafka (real-time streams)
- S3 (archived data) - not recommended

**For Data Engineering:**

- All platforms (need complete picture)
- Include pipelines and jobs
- Show lineage connections

</TabItem>
</Tabs>

### Entity Type Filtering

<div className="filter-guide">

**Filter by what you're looking for:**

| Need                  | Filter Selection      | Why                        |
| --------------------- | --------------------- | -------------------------- |
| **Raw Data**          | Datasets only         | Focus on tables and views  |
| **Business Insights** | Dashboards + Charts   | See existing analysis      |
| **Data Processing**   | Data Jobs + Pipelines | Understand transformations |
| **Complete Picture**  | All entity types      | Full ecosystem view        |

</div>

### Interactive Exercise: Smart Filtering

<div className="interactive-exercise">

**Challenge**: Find customer segmentation data suitable for marketing analysis

**Your Turn:**

1. **Search**: `customer segment`
2. **Apply Filters**:
   - Entity Type: "Datasets"
   - Platform: "Snowflake" OR "BigQuery"
   - (If available) Domain: "Marketing" or "Analytics"
3. **Compare**: How many results now vs. before filtering?

**Success Criteria**:

- Results reduced to manageable number (< 20)
- Results are more relevant to marketing use case
- You can see clear candidates for your analysis

</div>

## Level 3: Advanced Search Operators

Unlock DataHub's power with search operators:

### Boolean Operators

<Tabs>
<TabItem value="and-or" label="AND / OR Logic">

<div className="operator-examples">

**AND - All terms must match:**

```
customer AND segmentation AND marketing
```

_Finds datasets containing all three terms_

**OR - Any term can match:**

```
customer OR user OR client
```

_Finds datasets with any customer-related term_

**Combined Logic:**

```
(customer OR user) AND (segment OR cohort OR tier)
```

_Flexible matching for customer segmentation concepts_

</div>

</TabItem>
<TabItem value="not-exclude" label="NOT / Exclusion">

<div className="operator-examples">

**NOT - Exclude unwanted results:**

```
customer NOT test
```

_Customer data excluding test tables_

**Exclude Multiple Terms:**

```
customer NOT (test OR temp OR backup)
```

_Clean production customer data only_

**Exclude Platforms:**

```
customer NOT platform:mysql
```

_Customer data from all platforms except MySQL_

</div>

</TabItem>
</Tabs>

### Field-Specific Search

Target specific metadata fields for precision:

<div className="field-search-guide">

**Syntax**: `field:value` or `field:"exact phrase"`

| Field          | Example                                 | Use Case                  |
| -------------- | --------------------------------------- | ------------------------- |
| `name:`        | `name:customer*`                        | Search table/column names |
| `description:` | `description:"customer lifetime value"` | Search documentation      |
| `platform:`    | `platform:snowflake`                    | Specific data platform    |
| `tags:`        | `tags:pii`                              | Find tagged datasets      |
| `owners:`      | `owners:john.doe`                       | Find owned datasets       |

</div>

### Wildcard and Pattern Matching

<Tabs>
<TabItem value="wildcards" label="* Wildcards">

<div className="wildcard-examples">

**Prefix Matching:**

```
customer*
```

_Matches: customer, customers, customer_data, customer_analytics_

**Suffix Matching:**

```
*_customer
```

_Matches: dim_customer, fact_customer, raw_customer_

**Complex Patterns:**

```
cust*_seg*
```

_Matches: customer_segments, cust_data_segmentation_

</div>

</TabItem>
<TabItem value="exact-phrases" label="Exact Phrases">

<div className="phrase-examples">

**Exact Phrase Matching:**

```
"customer lifetime value"
```

_Must contain this exact phrase_

**Combine with Operators:**

```
"customer segmentation" OR "user cohorts"
```

_Either exact phrase_

**Field + Phrase:**

```
description:"high value customers"
```

_Exact phrase in description field_

</div>

</TabItem>
</Tabs>

### Interactive Exercise: Operator Mastery

<TutorialExercise
type="hands-on"
title="Progressive Search Builder Challenge"
difficulty="intermediate"
timeEstimate="8 minutes"
platform="DataHub UI"
steps={[
{
title: "Level 1: Basic Operators",
description: "Start with simple AND logic to find customer segmentation data",
code: "customer AND segment",
expected: "Find datasets containing both 'customer' and 'segment' keywords"
},
{
title: "Level 2: Add Exclusions",
description: "Exclude test data to focus on production datasets",
code: "customer AND segment NOT test",
expected: "Same results but filtered to exclude test/development data"
},
{
title: "Level 3: Field Targeting",
description: "Target specific fields for more precise results",
code: "name:customer* AND description:segment*",
expected: "Datasets with 'customer' in name and 'segment' in description"
},
{
title: "Level 4: Complex Logic",
description: "Combine multiple conditions with advanced grouping",
code: "(name:customer* OR name:user*) AND (description:segment* OR description:cohort*) AND platform:snowflake",
expected: "Snowflake datasets about customer/user segmentation or cohorts"
}
]}>

**Your Mission**: Try each level in DataHub and observe how results change. Notice how each level gives you more control and precision. Which approach gives you the most relevant results for marketing analysis?

**Pro Tip**: Copy each query into DataHub's search bar and compare the result quality. Level 4 should give you the most targeted, actionable datasets.

</TutorialExercise>

## Level 4: Saved Searches & Efficiency

Don't repeat work - save your successful searches:

### Creating Saved Searches

<div className="saved-search-guide">

**When you find a great search:**

1. **Perfect your search** using the techniques above
2. **Click the bookmark icon** next to the search bar
3. **Name it descriptively**: "Customer Segmentation - Marketing Ready"
4. **Add description**: "Analytics-ready customer segment data for marketing campaigns"
5. **Set sharing**: Team-wide or personal

**Pro Naming Convention:**

- `[Use Case] - [Data Type] - [Quality Level]`
- Examples:
  - "Marketing - Customer Segments - Production"
  - "Analysis - User Behavior - High Quality"
  - "Reporting - Sales Metrics - Daily Updated"

</div>

### Search Templates for Common Scenarios

<Tabs>
<TabItem value="marketing" label="Marketing Analysis">

```
# High-quality customer data for campaigns
(customer OR user) AND (segment OR cohort OR tier)
AND platform:(snowflake OR bigquery)
NOT (test OR temp OR backup)
```

</TabItem>
<TabItem value="operations" label="Operational Data">

```
# Live operational customer data
name:customer* AND platform:(postgres OR mysql)
AND hasOwners:true AND updatedInLastWeek:true
```

</TabItem>
<TabItem value="analytics" label="Analytics Ready">

```
# Processed analytical datasets
description:(analytics OR analysis OR processed)
AND (customer OR user) AND NOT raw
AND platform:(snowflake OR bigquery OR dbt)
```

</TabItem>
</Tabs>

## Success Checkpoint

<div className="checkpoint">

**You've mastered advanced search when you can:**

**Speed Test**: Find relevant customer segmentation data in under 2 minutes
**Precision Test**: Get < 10 highly relevant results using operators
**Efficiency Test**: Create and use a saved search for future use
**Strategy Test**: Choose the right approach for different discovery scenarios

**Validation Exercise:**
Try to solve this in 90 seconds: _"Find production-ready customer analytics data suitable for a marketing campaign, excluding any test or temporary tables."_

**Expected Result**: 1-5 highly relevant datasets from analytics platforms

</div>

## Pro Tips & Shortcuts

<div className="pro-tips">

**Speed Techniques:**

- Use browser bookmarks for common DataHub searches
- Set up browser shortcuts: `dh customer` → DataHub customer search
- Learn keyboard shortcuts: `Ctrl+K` for quick search

**Accuracy Boosters:**

- Always check the "Updated" date - stale data wastes time
- Look for owner information - contactable owners = reliable data
- Check description quality - well-documented data is usually better maintained

**Team Efficiency:**

- Share successful search patterns with teammates
- Create team-wide saved searches for common use cases
- Document search strategies in team wikis

</div>

## Troubleshooting Common Issues

<Tabs>
<TabItem value="too-many-results" label="Too Many Results">

**Problem**: Search returns hundreds of results

**Solutions:**

1. **Add more specific terms**: `customer segmentation marketing`
2. **Use field targeting**: `name:customer* AND description:segment*`
3. **Apply platform filters**: Focus on relevant data platforms
4. **Exclude noise**: `NOT (test OR temp OR backup OR old)`

</TabItem>
<TabItem value="no-results" label="No Results">

**Problem**: Search returns nothing

**Solutions:**

1. **Check spelling**: Try variations and wildcards
2. **Broaden terms**: Use OR operators for synonyms
3. **Remove filters**: Start broad, then narrow down
4. **Try different fields**: Maybe it's in descriptions, not names

</TabItem>
<TabItem value="wrong-results" label="Wrong Results">

**Problem**: Results aren't relevant to your use case

**Solutions:**

1. **Add context terms**: Include your domain/use case
2. **Use exclusions**: Remove irrelevant categories
3. **Filter by platform**: Match your analysis environment
4. **Check entity types**: Maybe you need dashboards, not datasets

</TabItem>
</Tabs>

## What You've Learned

**Congratulations!** You've transformed from basic search to advanced discovery:

- **Strategic Approach**: Business-first thinking with technical backup
- **Smart Filtering**: Platform and entity type filtering for relevance
- **Advanced Operators**: Boolean logic, field targeting, and wildcards
- **Efficiency Tools**: Saved searches and reusable patterns
- **Troubleshooting**: Common issues and systematic solutions

---

<NextStepButton to="./dataset-profiles">
Next: Understand and Evaluate Your Data
</NextStepButton>
