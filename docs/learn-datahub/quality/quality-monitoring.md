import DataHubEntityCard from '@site/src/components/DataHubEntityCard';

# Quality Monitoring

<TutorialProgress
currentStep="quality-monitoring"
steps={[
{ id: 'quality-overview', label: 'Overview', completed: true },
{ id: 'data-assertions', label: 'Data Assertions', completed: true },
{ id: 'quality-monitoring', label: 'Quality Monitoring', completed: false },
{ id: 'incident-management', label: 'Incident Management', completed: false },
{ id: 'quality-automation', label: 'Quality Automation', completed: false }
]}
compact={true}
/>

## Building Comprehensive Quality Dashboards

**Time Required**: 12 minutes

### The Monitoring Challenge

You've implemented data assertions, but you need visibility into quality trends across your entire data landscape:

- **Scattered quality information** across different systems and teams
- **Reactive approach** - discovering issues only when stakeholders complain
- **No quality trends** to identify deteriorating data sources
- **Lack of accountability** for quality improvements

**Real-World Impact**: Your CEO asked for a "data quality report" for the board meeting, but it took your team 3 days to manually gather quality metrics from various sources, and the information was already outdated by presentation time.

### Understanding Quality Monitoring

Quality monitoring provides continuous visibility into data health across your organization:

<DataHubLineageFlow
{...SampleLineageFlows.qualityMonitoringFlow}
showColumnLineage={false}
className="tutorial-lineage"
/>

**Monitoring Capabilities**:

- **Real-time Dashboards**: Live quality metrics across all data assets
- **Trend Analysis**: Historical quality patterns and improvement tracking
- **Quality Scorecards**: Domain-specific quality assessments
- **Proactive Alerting**: Early warning system for quality degradation
- **Executive Reporting**: Summary views for leadership and stakeholders

### Exercise 1: Create Quality Dashboards

Build comprehensive dashboards for different stakeholder needs:

#### Step 1: Executive Quality Dashboard

Create a high-level view for leadership:

1. **Navigate to Analytics** → **Quality Dashboards**
2. **Create New Dashboard**: "Executive Data Quality Overview"
3. **Add Key Metrics**:
   - Overall quality score (percentage of passing assertions)
   - Critical data assets health status
   - Quality trend over last 90 days
   - Top quality issues by business impact

**Executive Dashboard Preview**:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_data_pipeline"
    type="Table"
    platform="Snowflake"
    description="Customer data with comprehensive quality monitoring and validation"
    owners={[
      { name: 'data.quality@company.com', type: 'Data Steward' }
    ]}
    tags={['Critical', 'Customer-Data', 'Quality-Monitored']}
    glossaryTerms={['Customer Data', 'Quality Assured']}
    assertions={{ passing: 23, failing: 1, total: 24 }}
    health="Good"
  />
  
  <DataHubEntityCard 
    name="financial_transactions"
    type="Table"
    platform="PostgreSQL"
    description="Financial transaction data requiring immediate attention for quality issues"
    owners={[
      { name: 'finance.engineering@company.com', type: 'Technical Owner' }
    ]}
    tags={['Financial', 'Critical', 'Quality-Issues']}
    glossaryTerms={['Financial Data', 'Transaction Record']}
    assertions={{ passing: 18, failing: 4, total: 22 }}
    health="Warning"
  />
</div>

**Quality Metrics Summary**:

- **Overall Quality Score**: 94.2% ↑ (+2.1% vs last month)
- **Critical Assets**: Customer Data (98.5%), Financial Data (89.2% - needs attention)
- **Trending Issues**: Payment processing delays, email validation failures

#### Step 2: Operational Quality Dashboard

Create detailed views for data teams:

**Dashboard Configuration**:

- **Name**: "Data Engineering Quality Operations"
- **Refresh**: Every 5 minutes
- **Scope**: All production datasets

**Key Sections**:

1. **Real-time Assertion Status**
2. **Pipeline Quality Health**
3. **Data Freshness Monitoring**
4. **Quality Issue Queue**

### Exercise 2: Set Up Quality Scorecards

Create domain-specific quality assessments:

#### Step 1: Customer Domain Scorecard

**Scorecard Configuration**:

- **Domain**: Customer Data
- **Assets**: Customer profiles, transactions, interactions
- **Quality Dimensions**:
  - Completeness (weight: 30%)
  - Accuracy (weight: 25%)
  - Consistency (weight: 20%)
  - Timeliness (weight: 15%)
  - Validity (weight: 10%)

**Scoring Logic**:

```
Customer Domain Quality Score =
  (Completeness × 0.30) +
  (Accuracy × 0.25) +
  (Consistency × 0.20) +
  (Timeliness × 0.15) +
  (Validity × 0.10)
```

#### Step 2: Financial Domain Scorecard

**Enhanced Requirements for Financial Data**:

- **Regulatory Compliance**: SOX, GAAP adherence
- **Audit Trail**: Complete lineage and change tracking
- **Precision**: Exact decimal calculations
- **Reconciliation**: Cross-system balance validation

### Exercise 3: Implement Trend Analysis

Monitor quality patterns over time:

#### Step 1: Quality Trend Monitoring

**Trend Metrics to Track**:

- Daily assertion pass rates
- Weekly quality score changes
- Monthly quality improvement goals
- Quarterly compliance assessments

**Trend Analysis Queries**:

```sql
-- Daily quality trend
SELECT
  date,
  COUNT(CASE WHEN status = 'PASS' THEN 1 END) * 100.0 / COUNT(*) as pass_rate,
  COUNT(*) as total_assertions
FROM assertion_results
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date
ORDER BY date;

-- Quality improvement by domain
SELECT
  domain,
  AVG(CASE WHEN date >= CURRENT_DATE - INTERVAL '7 days' THEN quality_score END) as current_week,
  AVG(CASE WHEN date >= CURRENT_DATE - INTERVAL '14 days'
           AND date < CURRENT_DATE - INTERVAL '7 days' THEN quality_score END) as previous_week
FROM domain_quality_scores
GROUP BY domain;
```

#### Step 2: Seasonal Pattern Detection

Identify recurring quality patterns:

- **End-of-month** data processing spikes
- **Holiday periods** with reduced data volumes
- **Business cycle** impacts on data quality
- **System maintenance** windows affecting freshness

### Exercise 4: Create Quality Alerts

Set up intelligent alerting for quality issues:

#### Step 1: Threshold-Based Alerts

**Alert Configuration**:

- **Critical Alert**: Overall quality drops below 90%
- **Warning Alert**: Domain quality drops below 95%
- **Info Alert**: New quality issues detected

**Alert Channels**:

- Slack integration for immediate team notification
- Email summaries for daily quality reports
- PagerDuty integration for critical production issues
- Jira ticket creation for tracking resolution

#### Step 2: Anomaly Detection Alerts

**Statistical Alerting**:

```sql
-- Detect unusual assertion failure rates
WITH daily_stats AS (
  SELECT
    date,
    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) as failures,
    COUNT(*) as total
  FROM assertion_results
  WHERE date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY date
),
baseline AS (
  SELECT
    AVG(failures * 100.0 / total) as avg_failure_rate,
    STDDEV(failures * 100.0 / total) as stddev_failure_rate
  FROM daily_stats
  WHERE date < CURRENT_DATE
)
SELECT
  ds.date,
  ds.failures * 100.0 / ds.total as current_failure_rate,
  b.avg_failure_rate + (2 * b.stddev_failure_rate) as alert_threshold
FROM daily_stats ds, baseline b
WHERE ds.date = CURRENT_DATE
  AND ds.failures * 100.0 / ds.total > b.avg_failure_rate + (2 * b.stddev_failure_rate);
```

### Exercise 5: Build Quality Reports

Create automated reporting for stakeholders:

#### Step 1: Daily Quality Summary

**Automated Daily Report**:

- Overall quality status
- New issues discovered
- Issues resolved
- Quality trends
- Upcoming maintenance impacts

**Report Template**:

```
Daily Data Quality Report - [Date]

OVERALL STATUS
Quality Score: 94.2% (↑ 0.3% from yesterday)
Critical Issues: 2 (down from 5)
New Issues: 1
Resolved Issues: 4

DOMAIN BREAKDOWN
Customer Data: 96.1% (Good)
Financial Data: 89.2% (Warning - investigating payment delays)
Product Data: 95.8% (Good)
Marketing Data: 94.5% (Good)

ATTENTION REQUIRED
1. Payment processing latency (Financial) - ETA: 2PM
2. Customer email validation (CRM) - In progress

TRENDS
- 7-day average: 93.8% (improving)
- Month-to-date: 94.1% (on track for 95% goal)
```

#### Step 2: Executive Monthly Report

**Strategic Quality Report**:

- Quality ROI and business impact
- Quality initiative progress
- Resource allocation recommendations
- Compliance status updates

### Understanding Quality Metrics

**Key Performance Indicators (KPIs)**:

**Operational Metrics**:

- **Assertion Pass Rate**: Percentage of quality checks passing
- **Mean Time to Detection (MTTD)**: Speed of quality issue identification
- **Mean Time to Resolution (MTTR)**: Speed of quality issue fixes
- **Data Freshness**: Timeliness of data updates

**Business Metrics**:

- **Quality-Related Incidents**: Business disruptions due to data issues
- **Stakeholder Satisfaction**: User confidence in data quality
- **Compliance Score**: Adherence to regulatory requirements
- **Quality ROI**: Business value of quality improvements

### Advanced Monitoring Techniques

#### 1. Machine Learning-Enhanced Monitoring

Use ML to improve quality detection:

- **Anomaly Detection**: Identify unusual data patterns
- **Predictive Quality**: Forecast potential quality issues
- **Root Cause Analysis**: Automatically identify issue sources
- **Quality Recommendations**: Suggest improvement actions

#### 2. Real-Time Quality Streaming

Monitor quality in real-time data streams:

```sql
-- Real-time quality monitoring
SELECT
  window_start,
  COUNT(*) as records_processed,
  COUNT(CASE WHEN quality_check = 'PASS' THEN 1 END) as quality_records,
  COUNT(CASE WHEN quality_check = 'FAIL' THEN 1 END) as quality_failures
FROM streaming_quality_results
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY window_start
ORDER BY window_start DESC;
```

#### 3. Cross-System Quality Correlation

Monitor quality across integrated systems:

- Correlate quality issues with system performance
- Identify upstream causes of quality problems
- Track quality impact propagation
- Coordinate quality improvements across teams

### Quality Monitoring Best Practices

#### 1. Design for Different Audiences

- **Executives**: High-level trends and business impact
- **Data Teams**: Detailed technical metrics and alerts
- **Business Users**: Domain-specific quality insights
- **Compliance**: Regulatory adherence tracking

#### 2. Balance Detail and Usability

- Start with key metrics, add detail as needed
- Use visual indicators for quick status assessment
- Provide drill-down capabilities for investigation
- Include contextual information and recommendations

#### 3. Ensure Actionability

- Link quality metrics to specific improvement actions
- Provide clear ownership and escalation paths
- Include remediation guidance and documentation
- Track improvement progress over time

### Next Steps

With comprehensive quality monitoring in place, you're ready to implement incident management processes that ensure rapid response to quality issues.

<NextStepButton to="./incident-management" />
