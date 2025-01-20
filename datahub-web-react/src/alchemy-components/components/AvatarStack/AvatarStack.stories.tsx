import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import { AvatarStack, avatarListDefaults } from './AvatarStack';

// Meta Configuration
const meta = {
    title: 'Components / AvatarStack',
    component: AvatarStack,

    // Component-level parameters
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Displays a list of assignees with avatars.',
        },
    },

    // Component-level argTypes
    argTypes: {
        avatars: {
            description: 'List of avatar objects with name and image source.',
            control: 'object',
            table: {
                type: {
                    summary: 'Array<{ name: string; imageUrl: string }>',
                },
                defaultValue: { summary: '[]' },
            },
        },
    },

    // Default props
    args: {
        avatars: avatarListDefaults.avatars,
        size: avatarListDefaults.size,
    },
} satisfies Meta<typeof AvatarStack>;

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {
    render: (props) => <AvatarStack {...props} />,
};

// Example Stories
export const withMultipleAvatar = () => (
    <AvatarStack
        avatars={[
            { name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
            { name: 'Test User', imageUrl: 'https://robohash.org/sample-profile.png' },
            { name: 'Micky Test', imageUrl: 'https://randomuser.me/api/portraits/women/1.jpg' },
        ]}
        size="md"
    />
);

// Example Stories
export const witouthImagesMultipleAvatar = () => (
    <AvatarStack
        avatars={[
            { name: 'John Doe', imageUrl: null },
            { name: 'Test User', imageUrl: null },
            { name: 'Micky Test', imageUrl: null },
        ]}
        size="md"
    />
);

export const withSingleAvatar = () => (
    <AvatarStack avatars={[{ name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' }]} />
);

export const withNoAvatar = () => <AvatarStack avatars={[]} />;
