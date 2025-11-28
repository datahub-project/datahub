import TutorialProgress from '@site/src/components/TutorialProgress';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import NextStepButton from '@site/src/components/NextStepButton';
import ProcessFlow from '@site/src/components/ProcessFlow';
import { HandsOnExercise } from '@site/src/components/TutorialExercise';

# Privacy Controls (12 minutes)

<TutorialProgress
tutorialId="privacy"
currentStep={1}
steps={[
{ title: 'PII Detection', time: '12 min', description: 'Discover personal data across systems' },
{ title: 'Privacy Controls', time: '12 min', description: 'Implement protection and access controls' },
{ title: 'Compliance Workflows', time: '11 min', description: 'Automate DSARs and regulatory reporting' }
]}
/>

## Objective

Implement practical privacy protections including minimization, access controls, and retention.

<ProcessFlow
title="Privacy Protection Workflow"
steps={[
{ title: 'Identify PII', description: 'Use classifications to scope controls' },
{ title: 'Minimize & Mask', description: 'Limit fields and apply masking' },
{ title: 'Access Controls', description: 'Roles, approvals, and purpose limits' },
{ title: 'Retention', description: 'Define and enforce retention windows' }
]}
type="horizontal"
animated={true}
/>

## Control Implementation

<Tabs>
<TabItem value="minimization" label="Minimization & Masking">

- Remove unnecessary personal fields
- Apply field-level masking for sensitive attributes

</TabItem>
<TabItem value="access" label="Access Controls">

- Restrict PII access to approved roles
- Require purpose-based approvals for sensitive data

</TabItem>
<TabItem value="retention" label="Retention">

- Define retention policies per data category
- Configure automated deletion after retention window

</TabItem>
</Tabs>

## Hands-On Exercise

<HandsOnExercise
title="Apply Practical Controls"
difficulty="intermediate"
timeEstimate="5 min"
steps={[
{
title: 'Minimize schema',
description: 'Remove or mask non-essential personal attributes'
},
{
title: 'Set access policy',
description: 'Restrict read access to approved roles and add approval notes'
},
{
title: 'Define retention',
description: 'Document retention period and automate deletion where possible'
}
]}
/>

<NextStepButton to="./compliance-workflows">
Next: Compliance Workflows
</NextStepButton>
