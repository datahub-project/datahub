# Chat Traffic Simulator

Simulate realistic chat conversations to test and demonstrate the observability dashboard's real-time monitoring capabilities.

## Quick Start

### 1. Start the Integrations Service

```bash
# In terminal 1
uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 --reload
```

### 2. Start the Observability Dashboard

```bash
# In terminal 2
./experiments/observability/scripts/run_dashboard.sh
# Opens at http://localhost:8501
```

### 3. Enable Auto-Refresh

In the dashboard:

- ✅ Check "Auto-refresh (5s)"
- Select "chat" from AI Module dropdown
- Choose "Rate (per minute)" chart type

### 4. Generate Traffic

```bash
# In terminal 3 - Quick test (5 messages)
./experiments/observability/scripts/test_traffic_quick.sh

# OR - Continuous traffic generation
./experiments/observability/scripts/test_traffic_continuous.sh
```

### 5. Watch the Dashboard Update!

You'll see in real-time:

- 📈 User Messages incrementing
- 🤖 LLM Calls per message (typically 2-3)
- 🔧 Tool Calls showing search activity
- 💰 Cost accumulation
- 📊 Rate charts showing activity spikes

## Usage Options

### Pre-built Scripts

#### Quick Test (Recommended for First Try)

```bash
./experiments/observability/scripts/test_traffic_quick.sh
```

- Sends 5 messages
- 2 second delays
- Mixed scenario questions
- ~15 seconds total

#### Continuous Monitoring

```bash
./experiments/observability/scripts/test_traffic_continuous.sh
```

- Runs indefinitely (Ctrl+C to stop)
- 5 second delays
- Perfect for long-term monitoring
- Generates steady traffic

### Direct Script Usage

#### Basic Examples

```bash
# Send 10 messages with 3 second delays
python experiments/observability/simulate_chat_traffic.py \
    --messages 10 --delay 3

# Run continuously with 5 second delays
python experiments/observability/simulate_chat_traffic.py \
    --continuous --delay 5

# Quick burst test (1 second delays)
python experiments/observability/simulate_chat_traffic.py \
    --messages 5 --delay 1
```

#### Scenario-Specific Traffic

```bash
# Dataset exploration scenario
python experiments/observability/simulate_chat_traffic.py \
    --messages 10 --scenario exploration

# Troubleshooting scenario
python experiments/observability/simulate_chat_traffic.py \
    --messages 10 --scenario troubleshooting

# Documentation generation scenario
python experiments/observability/simulate_chat_traffic.py \
    --messages 10 --scenario documentation

# Investigation scenario
python experiments/observability/simulate_chat_traffic.py \
    --messages 10 --scenario investigation

# Mixed scenarios (default)
python experiments/observability/simulate_chat_traffic.py \
    --messages 10 --scenario mixed
```

#### Connect to Remote Service

```bash
# Use environment variable
export INTEGRATIONS_SERVICE_URL="https://dev01.acryl.io/integrations"
export DATAHUB_GMS_API_TOKEN="your-token-here"
python experiments/observability/simulate_chat_traffic.py --messages 5

# Or use command line arguments
python experiments/observability/simulate_chat_traffic.py \
    --messages 5 \
    --service-url "https://dev01.acryl.io/integrations" \
    --token "your-token-here"
```

## Conversation Scenarios

The simulator includes 5 realistic scenarios:

### 1. Exploration

Dataset discovery and browsing questions:

- "What datasets are available?"
- "Show me the most popular datasets"
- "Which tables have the most documentation?"
- "What are the top viewed dashboards?"
- "Find datasets related to user analytics"

### 2. Investigation

Finding specific data and metadata:

- "Show me datasets that were recently updated"
- "Which tables have high query volume?"
- "Find datasets owned by the data platform team"
- "What datasets are tagged as 'pii'?"
- "Show me the lineage for the users table"

### 3. Troubleshooting

Debugging data issues:

- "Why is the revenue dashboard showing no data?"
- "Which datasets depend on the events table?"
- "Show me failed data quality checks"
- "Find datasets with schema changes in the last week"
- "What's the refresh schedule for the analytics database?"

### 4. Documentation

Generating descriptions:

- "Generate a description for the customers dataset"
- "What does the user_events table contain?"
- "Explain the purpose of the staging database"
- "Summarize the data warehouse schema"
- "What columns are in the orders table?"

### 5. Mixed (Default)

Combination of all scenarios for realistic varied traffic.

## Output Format

The simulator provides real-time feedback:

```
======================================================================
🤖 DataHub Chat Traffic Simulator
======================================================================
Service URL: http://localhost:9003
Dashboard:   http://localhost:8501 (if running)
Metrics:     http://localhost:9003/metrics
======================================================================

[15:30:45] Message 1: What datasets are available?
  ✓ Success (2.34s, 5 events)
  Preview: I found several datasets in your DataHub instance...
  Stats: 1 success, 0 errors, 100.0% success rate, 2.34s avg

[15:30:48] Message 2: Show me the most popular datasets
  ✓ Success (3.12s, 7 events)
  Preview: Based on view counts, here are the most popular...
  Stats: 2 success, 0 errors, 100.0% success rate, 2.73s avg

...

======================================================================
📊 Simulation Summary
======================================================================
Total Messages:   5
Successful:       5
Failed:           0
Success Rate:     100.0%
Avg Duration:     2.85s
Total Duration:   14.25s
======================================================================

💡 Check the observability dashboard for metrics:
   - User Messages should show 5 total
   - LLM Calls should show multiple per message
   - Tool Calls should show search/retrieval activity
   - Time-series charts should show the spike in activity
```

## What Gets Tracked

Each message generates these metrics:

### Tier 1: User Messages

- 1 user message per chat request
- Success or error status
- Visible in "User Messages" metric

### Tier 2: LLM Calls

- Typically 2-4 LLM calls per message
- Initial reasoning + tool calls
- Visible in "LLM Calls" metric

### Tier 3: Tool Calls

- Search tools (dataset search, etc.)
- Metadata retrieval tools
- Visible in "Tool Calls" breakdown

### Cost Tracking

- Token usage (prompt + completion)
- Cost per model
- Accumulated in "Cost" metric

## Monitoring Dashboard Behavior

### Real-Time Updates (Auto-Refresh Enabled)

With auto-refresh on (5s interval), you'll see:

1. **Current Snapshot Metrics** (top section)

   - User Messages count incrementing
   - LLM Calls increasing (multiple per message)
   - Tool Calls showing search activity
   - Cost accumulating

2. **Time-Series Charts** (middle section)
   - Rate charts showing messages/min spike
   - Clear correlation between traffic bursts
   - Tool call breakdown over time

### Testing Scenarios

#### Scenario 1: Steady Traffic

```bash
python simulate_chat_traffic.py --continuous --delay 10
```

**What to watch:**

- Rate charts show consistent ~6 messages/min
- Cumulative values steadily increasing
- No spikes or drops

#### Scenario 2: Burst Traffic

```bash
python simulate_chat_traffic.py --messages 20 --delay 1
```

**What to watch:**

- Sharp spike in rate charts
- Rapid increase in cumulative metrics
- High messages/min during burst
- Gradual return to baseline

#### Scenario 3: Service Restart Detection

```bash
# Start continuous traffic
python simulate_chat_traffic.py --continuous --delay 5

# In another terminal, restart the service
# Kill and restart: uvicorn datahub_integrations.server:app ...
```

**What to watch:**

- Warning: "⚠️ Detected 1 service restart(s)"
- Raw cumulative chart drops to 0
- Rate chart automatically handles reset
- Adjusted cumulative maintains continuity

#### Scenario 4: Dashboard Gap Detection

```bash
# Start continuous traffic
python simulate_chat_traffic.py --continuous --delay 5

# Stop the dashboard (Ctrl+C)
# Wait 30 seconds
# Restart the dashboard
```

**What to watch:**

- Warning: "⚠️ Detected 1 monitoring gap(s)"
- Gap visible in time-series
- Data preserved before and after gap

## Performance Testing

### Load Testing

Generate high traffic to test system limits:

```bash
# Moderate load: 1 message per second
python simulate_chat_traffic.py --continuous --delay 1

# High load: Multiple simultaneous sessions
for i in {1..5}; do
    python simulate_chat_traffic.py --continuous --delay 2 &
done
# Monitor dashboard for:
# - Response times increasing
# - Success rate dropping
# - Queue buildup
```

### Stress Testing

```bash
# Sustained high load
python simulate_chat_traffic.py --continuous --delay 0.5
```

Monitor for:

- Memory usage in dashboard
- Response latency
- Error rates
- Resource utilization

## Troubleshooting

### Connection Errors

```
✗ Error: Connection refused
```

**Solution:** Ensure integrations service is running:

```bash
ps aux | grep uvicorn
# Should show process on port 9003
```

### Authentication Errors

```
✗ Error: HTTP 401: Unauthorized
```

**Solution:** Check token:

```bash
export DATAHUB_GMS_API_TOKEN="your-token-here"
```

### No Metrics Showing

**Problem:** Sent messages but dashboard shows 0

**Solutions:**

1. Check auto-refresh is enabled
2. Verify correct AI module selected ("chat")
3. Ensure service URL matches
4. Check time window includes recent activity

### Slow Responses

**Problem:** Messages taking >10 seconds

**Possible causes:**

- Service overloaded (check other traffic)
- Database slow (check underlying data stores)
- LLM provider throttling
- Network latency

## Advanced Usage

### Custom Scenarios

Create your own question set:

```python
# In simulate_chat_traffic.py, add:
SCENARIOS["custom"] = [
    "Your custom question 1",
    "Your custom question 2",
    "Your custom question 3",
]
```

### Integration with Testing

Use in CI/CD for performance regression testing:

```bash
#!/bin/bash
# Run traffic and check metrics
python simulate_chat_traffic.py --messages 10 --delay 2

# Query metrics endpoint
curl -s http://localhost:9003/metrics | grep genai_user_messages_total
# Verify expected values
```

### Monitoring Multiple Services

```bash
# Terminal 1: Monitor production
export INTEGRATIONS_SERVICE_URL="https://prod.example.com"
python simulate_chat_traffic.py --continuous --delay 10

# Terminal 2: Monitor staging
export INTEGRATIONS_SERVICE_URL="https://staging.example.com"
python simulate_chat_traffic.py --continuous --delay 10

# Dashboard shows metrics for currently configured service
```

## Tips

1. **Start Simple**: Use `test_traffic_quick.sh` first
2. **Enable Auto-Refresh**: See real-time updates
3. **Watch Rate Charts**: Best for seeing activity patterns
4. **Monitor Success Rate**: Should be >95% normally
5. **Check Response Times**: Should be <5s typically
6. **Use Scenarios**: Match your testing needs
7. **Combine with Dashboard**: Run side-by-side for best visibility

## See Also

- [TIMESERIES_ANALYTICS.md](./TIMESERIES_ANALYTICS.md) - Time-series features
- [run_dashboard.sh](../scripts/run_dashboard.sh) - Start dashboard
