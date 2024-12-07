import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';

import { GridList } from '@components/.docs/mdx-components';
import { Icon, iconDefaults, AVAILABLE_ICONS } from '.';

// Auto Docs
const meta = {
    title: 'Media / Icon',
    component: Icon,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: ['productionReady'],
        docs: {
            subtitle: 'A singular component for rendering the icons used throughout the application.',
            description: {
                component: '👉 See the [Icons Gallery](/docs/icons--docs) for more information.',
            },
        },
    },

    // Component-level argTypes
    argTypes: {
        icon: {
            description: `The name of the icon to display.`,
            type: 'string',
            options: AVAILABLE_ICONS,
            table: {
                defaultValue: { summary: 'undefined' },
            },
            control: {
                type: 'select',
            },
        },
        variant: {
            description: 'The variant of the icon to display.',
            defaultValue: 'outline',
            options: ['outline', 'filled'],
            table: {
                defaultValue: { summary: iconDefaults.variant },
            },
        },
        size: {
            description: 'The size of the icon to display.',
            defaultValue: 'lg',
            table: {
                defaultValue: { summary: iconDefaults.size },
            },
        },
        color: {
            description: 'The color of the icon to display.',
            options: ['inherit', 'white', 'black', 'violet', 'green', 'red', 'blue', 'gray'],
            type: 'string',
            table: {
                defaultValue: { summary: iconDefaults.color },
            },
            control: {
                type: 'select',
            },
        },
        rotate: {
            description: 'The rotation of the icon. Applies a CSS transformation.',
            table: {
                defaultValue: { summary: iconDefaults.rotate },
            },
        },
    },

    // Define defaults for required args
    args: {
        icon: iconDefaults.icon,
    },
} satisfies Meta<typeof Icon>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Icon {...props} />,
};

export const filled = () => (
    <GridList>
        <Icon icon="AccountCircle" variant="filled" />
        <Icon icon="AddHome" variant="filled" />
        <Icon icon="AdminPanelSettings" variant="filled" />
    </GridList>
);

export const sizes = () => (
    <GridList>
        <Icon icon={iconDefaults.icon} size="xs" />
        <Icon icon={iconDefaults.icon} size="sm" />
        <Icon icon={iconDefaults.icon} size="md" />
        <Icon icon={iconDefaults.icon} size="lg" />
        <Icon icon={iconDefaults.icon} size="xl" />
        <Icon icon={iconDefaults.icon} size="2xl" />
        <Icon icon={iconDefaults.icon} size="3xl" />
        <Icon icon={iconDefaults.icon} size="4xl" />
    </GridList>
);

export const colors = () => (
    <GridList>
        <Icon icon={iconDefaults.icon} color="white" />
        <Icon icon={iconDefaults.icon} color="black" />
        <Icon icon={iconDefaults.icon} color="violet" />
        <Icon icon={iconDefaults.icon} color="green" />
        <Icon icon={iconDefaults.icon} color="red" />
        <Icon icon={iconDefaults.icon} color="blue" />
        <Icon icon={iconDefaults.icon} color="gray" />
    </GridList>
);

export const rotation = () => (
    <GridList>
        <Icon icon="ChevronLeft" />
        <Icon icon="ChevronLeft" rotate="90" />
        <Icon icon="ChevronLeft" rotate="180" />
        <Icon icon="ChevronLeft" rotate="270" />
    </GridList>
);
