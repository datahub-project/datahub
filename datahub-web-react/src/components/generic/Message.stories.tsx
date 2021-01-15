import React from 'react';
import { Story, Meta } from '@storybook/react';
import { Message, MessageProps } from './Message';

export default {
    title: 'Library/Message',
    component: Message,
} as Meta;

const Template: Story<MessageProps> = (args) => (
    <div>
        <Message {...args} />
        <p>This is some text.</p>
    </div>
);

export const DefaultMessage = Template.bind({});
DefaultMessage.args = {
    type: 'loading',
    content: 'Sample message contents',
};
