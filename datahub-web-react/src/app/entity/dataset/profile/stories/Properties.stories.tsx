import React from 'react';
import { Story, Meta } from '@storybook/react';

import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { Properties, Props } from '../../../shared/Properties';
import { sampleProperties } from './properties';

export default {
    title: 'Dataset Profile / Properties',
    component: Properties,
} as Meta;

const Template: Story<Props> = (args) => <Properties {...args} />;

export const PropertyValues = Template.bind({});
PropertyValues.args = { properties: sampleProperties };
PropertyValues.decorators = [
    (InnerStory) => (
        <TestPageContainer>
            <InnerStory />
        </TestPageContainer>
    ),
];
