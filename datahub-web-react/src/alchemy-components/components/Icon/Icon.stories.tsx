import type { Meta, StoryObj } from '@storybook/react';
import { ChartLine } from '@phosphor-icons/react/dist/csr/ChartLine';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { ColorValues, FontSizeValues } from '@components/theme/config';

import { Icon, iconDefaults } from '.';

const storyDefaults = {
    icon: ChartLine,
};

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
                component: 'See the [Icons Gallery](/docs/icons--docs) for more information.',
            },
        },
    },

    // Component-level argTypes
    argTypes: {
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
        icon: ChartLine,
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

export const sizes = () => (
    <GridList>
        {Object.values(FontSizeValues).map((size) => (
            <Icon key={size} {...storyDefaults} size={size} />
        ))}
    </GridList>
);

export const colors = () => (
    <GridList>
        {Object.values(ColorValues).map((color) => (
            <Icon key={color} {...storyDefaults} color={color} />
        ))}
    </GridList>
);

export const weights = () => (
    <GridList>
        <Icon icon={ChartLine} weight="thin" size="4xl" />
        <Icon icon={ChartLine} weight="light" size="4xl" />
        <Icon icon={ChartLine} weight="regular" size="4xl" />
        <Icon icon={ChartLine} weight="bold" size="4xl" />
        <Icon icon={ChartLine} weight="fill" size="4xl" />
        <Icon icon={ChartLine} weight="duotone" size="4xl" />
    </GridList>
);

export const rotation = () => (
    <GridList>
        <Icon icon={ChartLine} />
        <Icon icon={ChartLine} rotate="90" />
        <Icon icon={ChartLine} rotate="180" />
        <Icon icon={ChartLine} rotate="270" />
    </GridList>
);
