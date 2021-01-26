import React from 'react';
import { Story, Meta } from '@storybook/react';

import { MockedProvider } from '@apollo/client/testing';
import { LogIn, LogInProps } from './LogIn';

export default {
    title: 'DataHub/LogIn',
    component: LogIn,
} as Meta;

const Template: Story<LogInProps> = (args) => <LogIn {...args} />;

export const LogInScreen = Template.bind({});
LogInScreen.args = {};
LogInScreen.decorators = [
    (InnerStory) => (
        <MockedProvider mocks={[]}>
            <InnerStory />
        </MockedProvider>
    ),
];
