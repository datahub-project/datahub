import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { GridList } from '@src/alchemy-components/.docs/mdx-components';
import { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Avatar, avatarDefaults } from './Avatar';

const IMAGE_URL =
    'https://is1-ssl.mzstatic.com/image/thumb/Purple211/v4/78/cb/e1/78cbe16d-28d9-057e-9f73-524c32eb5fe5/AppIcon-0-0-1x_U007emarketing-0-7-0-85-220.png/512x512bb.jpg';

// Auto Docs
const meta = {
    title: 'Components / Avatar',
    component: Avatar,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'This component allows users to render a user pill with picture and name',
        },
    },

    // Component-level argTypes
    argTypes: {
        name: {
            description: 'Name of the user.',
            table: {
                defaultValue: { summary: `${avatarDefaults.name}` },
            },
            control: 'text',
        },
        imageUrl: {
            description: 'URL of the user image.',
            control: 'text',
        },
        onClick: {
            description: 'On click function for the Avatar.',
        },
        size: {
            description: 'Size of the Avatar.',
            table: {
                defaultValue: { summary: `${avatarDefaults.size}` },
            },
            control: 'select',
        },
        showInPill: {
            description: 'Whether Avatar is shown in pill format with name.',
            table: {
                defaultValue: { summary: `${avatarDefaults.showInPill}` },
            },
            control: 'boolean',
        },

        isOutlined: {
            description: 'Whether Avatar is outlined.',
            table: {
                defaultValue: { summary: `${avatarDefaults.isOutlined}` },
            },
            control: 'boolean',
        },
    },

    // Define defaults
    args: {
        name: 'John Doe',
        size: 'default',
        showInPill: false,
        isOutlined: false,
    },
} satisfies Meta<typeof Avatar>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Avatar {...props} />,
};

export const sizes = () => (
    <GridList>
        <Avatar name="John Doe" size="lg" />
        <Avatar name="John Doe" size="md" />
        <Avatar name="John Doe" size="default" />
        <Avatar name="John Doe" size="sm" />
    </GridList>
);

export const withImage = () => (
    <GridList>
        <Avatar name="John Doe" imageUrl={IMAGE_URL} size="lg" />
        <Avatar name="John Doe" imageUrl={IMAGE_URL} size="md" />
        <Avatar name="John Doe" imageUrl={IMAGE_URL} size="default" />
        <Avatar name="John Doe" imageUrl={IMAGE_URL} size="sm" />
    </GridList>
);

export const pills = () => (
    <GridList isVertical>
        <GridList>
            <Avatar name="John Doe" size="lg" showInPill />
            <Avatar name="John Doe" size="md" showInPill />
            <Avatar name="John Doe" size="default" showInPill />
            <Avatar name="John Doe" size="sm" showInPill />
        </GridList>
        <GridList>
            <Avatar name="John Doe" size="lg" imageUrl={IMAGE_URL} showInPill />
            <Avatar name="John Doe" size="md" imageUrl={IMAGE_URL} showInPill />
            <Avatar name="John Doe" size="default" imageUrl={IMAGE_URL} showInPill />
            <Avatar name="John Doe" size="sm" imageUrl={IMAGE_URL} showInPill />
        </GridList>
    </GridList>
);

export const outlined = () => (
    <GridList>
        <Avatar name="John Doe" size="lg" imageUrl={IMAGE_URL} isOutlined />
        <Avatar name="John Doe" size="lg" showInPill imageUrl={IMAGE_URL} isOutlined />
    </GridList>
);

export const withOnClick = () => (
    <GridList>
        <Avatar name="John Doe" onClick={() => window.alert('Avatar clicked')} />
        <Avatar name="John Doe" onClick={() => window.alert('Avatar clicked')} showInPill />
    </GridList>
);
