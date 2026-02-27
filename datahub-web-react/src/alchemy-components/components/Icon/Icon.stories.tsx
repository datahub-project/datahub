import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { ColorValues, FontSizeValues } from '@components/theme/config';

import { AVAILABLE_ICONS, Icon, iconDefaults } from '.';

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
        weight: {
            description: 'The weight of the Phosphor icon.',
            defaultValue: 'regular',
            options: ['thin', 'light', 'regular', 'bold', 'fill', 'duotone'],
            table: {
                defaultValue: { summary: iconDefaults.weight },
            },
            control: {
                type: 'select',
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
        colorLevel: {
            description: 'The level of `color`',
            type: 'number',
            table: {
                defaultValue: { summary: '500' },
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
        icon: 'Activity',
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
        <Icon icon="User" weight="fill" />
        <Icon icon="House" weight="fill" />
        <Icon icon="Gear" weight="fill" />
    </GridList>
);

export const sizes = () => (
    <GridList>
        {Object.values(FontSizeValues).map((size) => (
            <Icon key={size} icon="Activity" size={size} />
        ))}
    </GridList>
);

export const colors = () => (
    <GridList>
        {Object.values(ColorValues).map((color) => (
            <Icon key={color} icon="Activity" color={color} />
        ))}
    </GridList>
);

export const rotation = () => (
    <GridList>
        <Icon icon="CaretLeft" />
        <Icon icon="CaretLeft" rotate="90" />
        <Icon icon="CaretLeft" rotate="180" />
        <Icon icon="CaretLeft" rotate="270" />
    </GridList>
);
