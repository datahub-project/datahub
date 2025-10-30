import TutorialProgress from '@site/src/components/TutorialProgress';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DataHubEntityCard from '@site/src/components/DataHubEntityCard';
import NextStepButton from '@site/src/components/NextStepButton';
import { HandsOnExercise } from '@site/src/components/TutorialExercise';

# PII Detection (12 minutes)

<TutorialProgress
tutorialId="privacy"
currentStep={0}
steps={[
{ title: 'PII Detection', time: '12 min', description: 'Discover personal data across systems' },
{ title: 'Privacy Controls', time: '12 min', description: 'Implement protection and access controls' },
{ title: 'Compliance Workflows', time: '11 min', description: 'Automate DSARs and regulatory reporting' }
]}
/>

## Objective

Identify personal data across platforms using automated PII detection and classification, establishing comprehensive visibility into privacy-relevant assets.

## What You'll Learn

- Configure and run PII detection
- Interpret classification results in UI
- Prioritize remediation using tags and terms

## PII Discovery Workflow

**Scan** → **Classify** → **Review** → **Tag** → **Monitor**

<Tabs>
<TabItem value="discover" label="Discover">

1. Navigate to a dataset with personal data (e.g., `customer_profiles`).
2. Open the Schema tab to review detected fields.
3. Confirm sensitive columns (e.g., `email`, `phone`).

</TabItem>
<TabItem value="classify" label="Classify">

1. Apply appropriate sensitivity tags and glossary terms.
2. Ensure PII classifications are consistent across similar datasets.

</TabItem>
</Tabs>

## Example Asset

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_profiles"
    type="Table"
    platform="Snowflake"
    description="Customer personal information including names, emails, and addresses"
    owners={[{ name: 'Privacy Team', type: 'Business Owner' }]}
    tags={['PII', 'Confidential', 'GDPR']}
    glossaryTerms={['Personal Data', 'Data Subject']}
    health="Good"
  />
</div>

## Hands-On Exercise

<HandsOnExercise
title="Run a PII Review"
difficulty="beginner"
timeEstimate="4 min"
steps={[
{
title: 'Open customer_profiles',
description: 'Review schema fields and detected classifications'
},
{
title: 'Apply tags & terms',
description: 'Ensure PII tags and glossary terms are applied consistently'
},
{
title: 'Capture owners',
description: 'Set technical and business owners for accountability'
}
]}
/>

<NextStepButton href="./privacy-controls">
Next: Privacy Controls
</NextStepButton>
