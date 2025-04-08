import { AVAILABLE_ICONS } from '@components';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { ButtonVariantValues } from '@components/components/Button/types';
import { MATERIAL_UI_ICONS, PHOSPHOR_ICONS } from '@components/components/Icon/constants';
import { SizeValues } from '@components/theme/config';

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
            options: Object.values(ButtonVariantValues),
            table: {
                defaultValue: { summary: buttonDefaults.variant },
            },
            control: {
                type: 'radio',
            },
        },
        color: {
            description: 'The color of the Button.',
            options: ['violet', 'green', 'red', 'gray'],
            table: {
                defaultValue: { summary: buttonDefaults.color },
            },
            control: {
                type: 'select',
            },
        },
        size: {
            description: 'The size of the Button.',
            options: Object.values(SizeValues),
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
            mapping: Object.fromEntries([
                ...MATERIAL_UI_ICONS.map((icon) => [icon, { icon, source: 'material', size: '2xl' }]),
                ...PHOSPHOR_ICONS.map((icon) => [icon, { icon, source: 'phosphor', size: '2xl' }]),
            ]),
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
        children: 'Button',
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
    render: (props) => <Button {...props} />,
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
        <Button size="xs">XSmall</Button>
        <Button size="sm">Small</Button>
        <Button size="md">Regular</Button>
        <Button size="lg">Large</Button>
        <Button size="xl">XLarge</Button>
    </GridList>
);

export const withIcon = () => (
    <GridList>
        <Button icon={{ icon: 'Add', source: 'material' }}>Icon Left</Button>
        <Button icon={{ icon: 'Add', source: 'material' }} iconPosition="right">
            Icon Right
        </Button>
    </GridList>
);

export const circleShape = () => (
    <GridList>
        <Button icon={{ icon: 'Add', source: 'material' }} size="sm" isCircle />
        <Button icon={{ icon: 'Add', source: 'material' }} isCircle />
        <Button icon={{ icon: 'Add', source: 'material' }} size="lg" isCircle />
    </GridList>
);
