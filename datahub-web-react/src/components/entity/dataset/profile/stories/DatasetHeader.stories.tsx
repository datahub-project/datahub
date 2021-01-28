import React from 'react';
import { Story, Meta } from '@storybook/react';

import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import DatasetHeader, { Props } from '../DatasetHeader';
import { sampleDataset, sampleDeprecatedDataset } from './sampleDataset';

export default {
    title: 'Dataset Profile / DatasetHeader',
    component: DatasetHeader,
} as Meta;

const Template: Story<Props> = (args) => <DatasetHeader {...args} />;

export const DescriptionAndOwner = Template.bind({});
DescriptionAndOwner.args = { dataset: sampleDataset };
DescriptionAndOwner.decorators = [
    (InnerStory) => (
        <>
            <TestPageContainer>
                <InnerStory />
            </TestPageContainer>
        </>
    ),
];

export const Deprecated = Template.bind({});
Deprecated.args = { dataset: sampleDeprecatedDataset };
Deprecated.decorators = [
    (InnerStory) => (
        <>
            <TestPageContainer>
                <InnerStory />
            </TestPageContainer>
        </>
    ),
];
