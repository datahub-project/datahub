import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import { AvatarList, avatarListDefaults } from './AvatarList';

// Meta Configuration
const meta = {
    title: 'Components / AvatarList',
    component: AvatarList,

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
} satisfies Meta<typeof AvatarList>;

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {
    render: (props) => <AvatarList {...props} />,
};

// Example Stories
export const withMultipleAvatar = () => (
    <AvatarList
        avatars={[
            { name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' },
            { name: 'Test User', imageUrl: 'https://robohash.org/sample-profile.png' },
            { name: 'Micky Test', imageUrl: 'https://randomuser.me/api/portraits/women/1.jpg' }, // No image URL
        ]}
        size="md"
    />
);

// Example Stories
export const witouthImagesMultipleAvatar = () => (
    <AvatarList
        avatars={[
            { name: 'John Doe', imageUrl: '' },
            { name: 'Test User', imageUrl: '' },
            { name: 'Micky Test', imageUrl: '' }, // No image URL
        ]}
        size="md"
    />
);

export const withSingleAvatar = () => (
    <AvatarList avatars={[{ name: 'John Doe', imageUrl: 'https://randomuser.me/api/portraits/men/1.jpg' }]} />
);

export const withNoAvatar = () => <AvatarList avatars={[]} />;
