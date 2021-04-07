import React from 'react';
import { Story, Meta } from '@storybook/react';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { sampleDocs } from './documentation';
import Documentation, { Props } from '../Documentation';

export default {
    title: 'Dataset Profile / Documentation',
    component: Documentation,
} as Meta;

const Template: Story<Props> = (args) => <Documentation {...args} />;

export const Documents = Template.bind({});
Documents.args = { documents: sampleDocs };
Documents.decorators = [
    (InnerStory) => (
        <TestPageContainer>
            <InnerStory />
        </TestPageContainer>
    ),
];
