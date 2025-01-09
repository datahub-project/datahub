import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import { GridList } from '@components/.docs/mdx-components';
import { AVAILABLE_ICONS } from '@components';

import { Button, buttonDefaults } from '.';

// Auto Docs
const meta = {
    title: 'Forms / Button',
    component: Button,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle:
                'Buttons are used to trigger an action or event, such as submitting a form, opening a dialog, canceling an action, or performing a delete operation.',
        },
    },

    // Component-level argTypes
    argTypes: {
        children: {
            description: 'The content of the Button.',
            control: {
                type: 'text',
            },
        },
        variant: {
            description: 'The variant of the Button.',
            options: ['filled', 'outline', 'text'],
            table: {
                defaultValue: { summary: buttonDefaults.variant },
            },
            control: {
                type: 'radio',
            },
        },
        color: {
            description: 'The color of the Button.',
            options: ['violet', 'green', 'red', 'blue', 'gray'],
            table: {
                defaultValue: { summary: buttonDefaults.color },
            },
            control: {
                type: 'select',
            },
        },
        size: {
            description: 'The size of the Button.',
            options: ['sm', 'md', 'lg', 'xl'],
            table: {
                defaultValue: { summary: buttonDefaults.size },
            },
            control: {
                type: 'select',
            },
        },
        icon: {
            description: 'The icon to display in the Button.',
            type: 'string',
            options: AVAILABLE_ICONS,
            table: {
                defaultValue: { summary: 'undefined' },
            },
            control: {
                type: 'select',
            },
        },
        iconPosition: {
            description: 'The position of the icon in the Button.',
            options: ['left', 'right'],
            table: {
                defaultValue: { summary: buttonDefaults.iconPosition },
            },
            control: {
                type: 'radio',
            },
        },
        isCircle: {
            description:
                'Whether the Button should be a circle. If this is selected, the Button will ignore children content, so add an Icon to the Button.',
            table: {
                defaultValue: { summary: buttonDefaults?.isCircle?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isLoading: {
            description: 'Whether the Button is in a loading state.',
            table: {
                defaultValue: { summary: buttonDefaults?.isLoading?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isDisabled: {
            description: 'Whether the Button is disabled.',
            table: {
                defaultValue: { summary: buttonDefaults?.isDisabled?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isActive: {
            description: 'Whether the Button is active.',
            table: {
                defaultValue: { summary: buttonDefaults?.isActive?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        onClick: {
            description: 'Function to call when the button is clicked',
            table: {
                defaultValue: { summary: 'undefined' },
            },
            action: 'clicked',
        },
    },

    // Define defaults
    args: {
        children: 'Button Content',
        variant: buttonDefaults.variant,
        color: buttonDefaults.color,
        size: buttonDefaults.size,
        icon: undefined,
        iconPosition: buttonDefaults.iconPosition,
        isCircle: buttonDefaults.isCircle,
        isLoading: buttonDefaults.isLoading,
        isDisabled: buttonDefaults.isDisabled,
        isActive: buttonDefaults.isActive,
        onClick: () => console.log('Button clicked'),
    },
} satisfies Meta<typeof Button>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Button {...props}>Button</Button>,
};

export const states = () => (
    <GridList>
        <Button>Default</Button>
        <Button isLoading>Loading State</Button>
        <Button isActive>Active/Focus State</Button>
        <Button isDisabled>Disabled State</Button>
    </GridList>
);

export const colors = () => (
    <GridList>
        <Button>Violet Button</Button>
        <Button color="green">Green Button</Button>
        <Button color="red">Red Button</Button>
        <Button color="blue">Blue Button</Button>
        <Button color="gray">Gray Button</Button>
    </GridList>
);

export const sizes = () => (
    <GridList>
        <Button size="sm">Small Button</Button>
        <Button size="md">Regular Button</Button>
        <Button size="lg">Large Button</Button>
        <Button size="xl">XLarge Button</Button>
    </GridList>
);

export const withIcon = () => (
    <GridList>
        <Button icon="Add">Icon Left</Button>
        <Button icon="Add" iconPosition="right">
            Icon Right
        </Button>
    </GridList>
);

export const circleShape = () => (
    <GridList>
        <Button icon="Add" size="sm" isCircle />
        <Button icon="Add" isCircle />
        <Button icon="Add" size="lg" isCircle />
    </GridList>
);
