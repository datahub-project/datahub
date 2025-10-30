import TutorialProgress from '@site/src/components/TutorialProgress';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import NextStepButton from '@site/src/components/NextStepButton';
import ProcessFlow from '@site/src/components/ProcessFlow';

# Compliance Workflows (11 minutes)

<TutorialProgress
tutorialId="privacy"
currentStep={2}
steps={[
{ title: 'PII Detection', time: '12 min', description: 'Discover personal data across systems' },
{ title: 'Privacy Controls', time: '12 min', description: 'Implement protection and access controls' },
{ title: 'Compliance Workflows', time: '11 min', description: 'Automate DSARs and regulatory reporting' }
]}
/>

## Objective

Operationalize compliance for DSARs (access, deletion, portability) and regulatory reporting.

## Core Workflows

<ProcessFlow
title="DSAR Fulfillment Flow"
steps={[
{ title: 'Intake', description: 'Receive and authenticate request' },
{ title: 'Locate', description: 'Identify subject data across systems' },
{ title: 'Approve', description: 'Review policy and authorize actions' },
{ title: 'Fulfill', description: 'Export, delete, or port data' },
{ title: 'Audit', description: 'Log evidence and completion' }
]}
type="horizontal"
animated={true}
/>

<Tabs>
<TabItem value="dsar" label="Data Subject Requests">

- Locate a subject’s data across systems
- Standardize export, deletion, and portability steps

</TabItem>
<TabItem value="reporting" label="Regulatory Reporting">

- Generate audit artifacts: lineage, access logs, retention status
- Track review and approval history

</TabItem>
</Tabs>

<NextStepButton href="./overview">
Back to Privacy & Compliance Overview
</NextStepButton>
