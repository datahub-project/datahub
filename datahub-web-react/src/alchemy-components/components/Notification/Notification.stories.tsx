import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { NotificationStorybookWrapper } from '@components/components/Notification/NotificationStorybookWrapper';
import { NotificationType } from '@components/components/Notification/types';

// Auto Docs
const meta = {
    title: 'Utils / Notification',
    component: NotificationStorybookWrapper,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'A utility for displaying global notification messages to the user.',
        },
    },

    // Component-level argTypes
    argTypes: {
        notificationType: {
            description: 'The type of notification to display',
            control: 'select',
            options: Object.values(NotificationType),
            table: {
                type: { summary: 'NotificationType' },
                defaultValue: { summary: 'ERROR' },
            },
        },
        message: {
            description: 'The title of notification box',
            control: 'text',
            table: {
                type: { summary: 'ReactNode' },
                defaultValue: { summary: 'Notification Message' },
            },
        },
        description: {
            description: 'The content of notification box',
            control: 'text',
            table: {
                type: { summary: 'ReactNode' },
                defaultValue: { summary: 'Notification Description' },
            },
        },
        duration: {
            description:
                'Time in seconds before Notification is closed. When set to 0 or null, it will never be closed automatically',
            control: { type: 'number', min: 0 },
            table: {
                type: { summary: 'number' },
                defaultValue: { summary: '3' },
            },
        },
        placement: {
            description:
                'Position of Notification, can be one of top | topLeft | topRight | bottom | bottomLeft | bottomRight',
            control: 'select',
            options: ['topLeft', 'topRight', 'bottomLeft', 'bottomRight'],
            table: {
                type: { summary: 'string' },
                defaultValue: { summary: 'topRight' },
            },
        },
        buttonText: {
            description: 'The text to display on the button that triggers the notification',
            control: 'text',
            table: {
                type: { summary: 'string' },
                defaultValue: { summary: 'Show Notification' },
            },
        },
    },

    args: {
        buttonText: 'Show Notification',
        notificationType: NotificationType.ERROR,
        message: 'Notification Message',
        description: ' Notification Description',
        duration: 3,
    },
} satisfies Meta<typeof NotificationStorybookWrapper>;

export default meta;

// Stories
type Story = StoryObj<typeof meta>;

// Basic story to test the notification functionality
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <NotificationStorybookWrapper {...props} />,
};
