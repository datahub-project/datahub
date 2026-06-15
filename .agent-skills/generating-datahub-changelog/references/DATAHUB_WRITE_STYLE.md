# DataHub Writing Style Guide

This style guide ensures consistency across DataHub documentation, changelogs, release notes, and communications. Follow these guidelines for all written content.

---

## Basic Principles

- **Clarity over cleverness**: Technical accuracy comes first. Be precise.
- **Active voice**: "The connector extracts metadata" not "Metadata is extracted by the connector."
- **Concise**: Respect the reader's time. Cut unnecessary words.
- **Scannable**: Use headers, bullets, and formatting to aid quick reading.

---

## Product & Company References

### DataHub Terminology

| Correct                            | Incorrect                                |
| ---------------------------------- | ---------------------------------------- |
| DataHub                            | Datahub, Data Hub, datahub               |
| Acryl Data                         | Acryl, AcrylData                         |
| GMS (Generalized Metadata Service) | gms, Gms                                 |
| MCE (Metadata Change Event)        | mce                                      |
| MCL (Metadata Change Log)          | mcl                                      |
| URN                                | Urn, urn (when referring to the concept) |

### Entity Names (Always Capitalize)

- Dataset, DataJob, DataFlow, Dashboard, Chart
- CorpUser, CorpGroup, Domain, Glossary Term
- Tag, Container, Schema Field

### Connector/Source Names

Use the canonical name from the source system:

- **Correct**: Snowflake, BigQuery, dbt, Kafka, Looker, Tableau
- **Incorrect**: snowflake, Big Query, DBT, kafka

### Aspect Names

Use PascalCase when referring to aspects:

- SchemaMetadata, Ownership, GlobalTags, GlossaryTerms
- DatasetProperties, Status, BrowsePathsV2

---

## Grammar & Mechanics

### Abbreviations

- Spell out on first use: "Change Data Capture (CDC)"
- Common tech abbreviations need no expansion: API, SQL, JSON, YAML, CLI
- DataHub-specific: Always expand URN, MCE, MCL, GMS on first use per document

### Contractions

- **Documentation**: Avoid contractions (use "do not" not "don't")
- **Changelogs/Release Notes**: Contractions acceptable for conversational tone
- **Error Messages**: No contractions

### Oxford Comma

Always use the Oxford comma:

- **Correct**: "Snowflake, BigQuery, and Redshift"
- **Incorrect**: "Snowflake, BigQuery and Redshift"

---

## Capitalization

### General Rules

- Sentence case for headings: "How to configure the Snowflake connector"
- Title case for proper nouns and product names only
- Never ALL CAPS except for acronyms (API, SQL, URN)

### Technical Terms

| Capitalize               | Lowercase                 |
| ------------------------ | ------------------------- |
| Python, Java, TypeScript | boolean, string, integer  |
| Kafka, Elasticsearch     | schema, table, column     |
| GraphQL, REST            | endpoint, query, mutation |
| Docker, Kubernetes       | container, pod, service   |

### Feature Names

Capitalize DataHub feature names:

- Lineage, Ownership, Glossary, Domains
- Assertions, Incidents, Data Contracts
- Ingestion, Actions, Search

---

## Punctuation

### Dashes

- **Em dash (—)**: For breaks in thought—use sparingly
- **En dash (–)**: For ranges (v0.10.0–v0.12.0)
- **Hyphen (-)**: For compound modifiers (real-time processing, event-driven architecture)

### Code References

- Use backticks for: file names, config keys, CLI commands, code snippets
- Examples: `datahub ingest`, `platform_instance`, `schema.yml`

### Lists

- Use periods for complete sentences
- No periods for fragments or single items
- Maintain parallel structure

**Correct**:

```
Features:
- Automatic schema detection
- Incremental extraction
- Lineage capture
```

**Incorrect**:

```
Features:
- Automatically detects schema.
- Incremental extraction
- It captures lineage
```

---

## Numbers

### Spelling Out

- Spell out one through nine
- Use numerals for 10 and above
- Always use numerals with units: "5 MB", "3 seconds", "2 retries"

### Versions

- Use semantic versioning format: v0.14.0, not v0.14 or 0.14.0
- Version ranges: v0.10.0–v0.12.0

### Percentages

- Use numerals with percent symbol: 50%, not fifty percent
- No space between number and %

### Time Durations (Human-Friendly Format)

Always convert raw timestamps to the most readable unit. Use the largest appropriate unit that keeps the number between 1-100.

| Raw Value    | Human-Friendly         | Avoid              |
| ------------ | ---------------------- | ------------------ |
| 0.5 hours    | 30 minutes             | 0.5 hours          |
| 2.5 hours    | 2 hours 30 minutes     | 2.5 hours          |
| 48 hours     | 2 days                 | 48 hours           |
| 168 hours    | 1 week                 | 168 hours, 7 days  |
| 542 hours    | 3.2 weeks or 22.6 days | 542 hours          |
| 720 hours    | 1 month                | 720 hours, 30 days |
| 90 seconds   | 1.5 minutes            | 90 seconds         |
| 3600 seconds | 1 hour                 | 3600 seconds       |

**Conversion rules:**

- < 1 hour → use minutes
- 1-48 hours → use hours (or hours + minutes for precision)
- 2-14 days → use days
- 2-8 weeks → use weeks
- > 8 weeks → use months

**For statistics (median, average):**

- Round to one decimal place: "3.2 weeks" not "3.2142857 weeks"
- Provide context when helpful: "3.2 weeks (22.6 days)"

**Examples in changelogs:**

- **Merge Time**: Median 3.2 weeks, Average 3.9 weeks
- **Response time improved**: from 450ms to 120ms
- **Job duration**: 2 hours 15 minutes

---

## Changelog & Release Notes Style

### Tone

- Conversational but professional
- Celebrate contributions without being excessive
- Focus on user impact, not implementation details

### Structure

```markdown
## 🚨 Breaking Changes

- 🚨 **PR #XXXX** (Component): What changed and the migration step (#PR)

## 🌟 New Features

- **Feature Name** - Brief description of user benefit (#PR)

## 🐛 Bug Fixes

- **Area** - What was broken and now works (#PR)

## 🛠️ Other Improvements

- **Area** - Improvement and its benefit (#PR)
```

### PR References

- Always include PR numbers: (#15787)
- Credit contributors: @username
- Group related changes

### Emojis (Changelogs Only)

| Section          | Emoji |
| ---------------- | ----- |
| Breaking Changes | 🚨    |
| New Features     | 🌟    |
| Bug Fixes        | 🐛    |
| Documentation    | 📚    |
| DevOps/CI        | 🔧    |
| Security         | 🔒    |
| Performance      | ⚡    |
| Deprecation      | ⚠️    |

---

## Technical Documentation Style

### Code Examples

- Always provide working examples
- Include comments explaining non-obvious parts
- Show both minimal and complete configurations

```yaml
# Minimal configuration
source:
  type: snowflake
  config:
    account_id: "xy12345.us-east-1"

# Complete configuration with all options
source:
  type: snowflake
  config:
    account_id: "xy12345.us-east-1"
    platform_instance: "production"  # Optional: disambiguate multiple Snowflake accounts
    include_table_lineage: true      # Default: true
```

### Configuration Documentation

- Document every config option
- Show type and default value
- Explain when/why to use each option

### Error Messages

- Start with what went wrong
- Explain why it happened
- Suggest how to fix it

**Good**: "Connection failed: Unable to reach Snowflake at xy12345.us-east-1. Verify your account_id is correct and your network allows outbound connections on port 443."

**Bad**: "Error: Connection failed"

---

## Words & Phrases

### Preferred Terms

| Use       | Avoid                               |
| --------- | ----------------------------------- |
| connector | source (when referring to the code) |
| ingest    | pull, fetch, sync                   |
| emit      | push, send                          |
| entity    | object, item                        |
| aspect    | attribute, property                 |
| configure | setup, set up (as verb)             |
| metadata  | meta-data, meta data                |

### Avoid These

- "Simply" / "Just" / "Easy" - What's easy for you may not be for others
- "Obviously" / "Clearly" - If it were obvious, you wouldn't need to write it
- "Please note that" - Just state the information
- "In order to" - Use "to"
- "Utilize" - Use "use"

### Connector States

- **Running**: Currently executing
- **Succeeded**: Completed successfully
- **Failed**: Completed with errors
- **Cancelled**: Stopped by user
- **Pending**: Waiting to start

---

## Links

### Internal Links

- Use relative paths for docs: `../architecture/architecture.md`
- Link to specific sections: `#configuration-options`

### External Links

- Use descriptive text, not URLs: "[Snowflake documentation](https://docs.snowflake.com)" not "https://docs.snowflake.com"
- Keep link text 2-5 words
- Open external links in new tab (when platform supports)

### PR and Issue Links (Changelogs)

**Always use short, friendly link names** - never bare URLs:

| Link Type     | ✅ Correct                                                      | ❌ Avoid                                              |
| ------------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| GitHub PR     | [#15859](https://github.com/datahub-project/datahub/pull/15859) | https://github.com/datahub-project/datahub/pull/15859 |
| Linear ticket | [ING-1282](https://linear.app/acryl-data/issue/ING-1282)        | https://linear.app/acryl-data/issue/ING-1282          |
| GitHub issue  | [#1234](https://github.com/org/repo/issues/1234)                | Full URL                                              |
| Zendesk       | [Ticket #5977](https://support.zendesk.com/...)                 | Full URL                                              |

**Format patterns:**

- GitHub PRs: `[#NUMBER](url)` - e.g., `[#15859](https://...)`
- Linear tickets: `[IDENTIFIER](url)` - e.g., `[ING-1282](https://...)`
- Keep the identifier as the link text, not a description

**Examples in changelogs:**

```markdown
✅ Fixed DB2 ARM compatibility ([#15859](https://github.com/...)) - [ING-1372](https://linear.app/...)
❌ Fixed DB2 ARM compatibility (https://github.com/datahub-project/datahub/pull/15859)
```

---

## Inclusive Language

- Use "they/them" for unknown individuals
- Avoid gendered language: "the user...they" not "the user...he"
- Use "allowlist/denylist" not "whitelist/blacklist"
- Use "primary/replica" not "master/slave"

---

## Quick Reference

### Before Publishing Checklist

- [ ] Product names capitalized correctly (DataHub, not Datahub)
- [ ] Connector names match source system (BigQuery, not Big Query)
- [ ] Code in backticks
- [ ] PR numbers included for changelogs
- [ ] No "simply" or "just"
- [ ] Active voice used
- [ ] Oxford commas present
- [ ] Links are descriptive, not bare URLs
