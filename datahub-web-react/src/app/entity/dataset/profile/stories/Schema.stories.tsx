import React from 'react';
import { Story, Meta } from '@storybook/react';

import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import Schema, { Props } from '../schema/Schema';
import { sampleSchema, sampleSchemaWithTags } from './sampleSchema';

export default {
    title: 'Dataset Profile / Schema',
    component: Schema,
} as Meta;

const Template: Story<Props> = (args) => <Schema {...args} />;

export const MockSchema = Template.bind({});
MockSchema.args = { schema: sampleSchema };
MockSchema.decorators = [
    (InnerStory) => (
        <TestPageContainer>
            <InnerStory />
        </TestPageContainer>
    ),
];

export const MockSchemaWithTags = Template.bind({});
MockSchemaWithTags.args = { schema: sampleSchemaWithTags };
MockSchemaWithTags.decorators = [
    (InnerStory) => (
        <TestPageContainer>
            <InnerStory />
        </TestPageContainer>
    ),
];
