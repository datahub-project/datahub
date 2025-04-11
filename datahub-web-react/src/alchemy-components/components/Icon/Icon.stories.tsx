import { ColorValues, FontSizeValues } from '@components/theme/config';
import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';

import { GridList } from '@components/.docs/mdx-components';
import { Icon, iconDefaults, AVAILABLE_ICONS, IconProps } from '.';

const storyMaterialDefaults: Pick<IconProps, 'icon' | 'source'> = {
    icon: 'AccountCircle',
    source: 'material',
};
const storyDefaults: Pick<IconProps, 'icon' | 'source'> = {
    icon: 'Activity',
    source: 'phosphor',
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
        icon: 'Activity',
        source: 'phosphor',
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
    <>
        <GridList>
            {Object.values(FontSizeValues).map((size) => (
                <Icon key={size} {...storyDefaults} size={size} />
            ))}
        </GridList>
        <GridList>
            {Object.values(FontSizeValues).map((size) => (
                <Icon key={size} {...storyMaterialDefaults} size={size} />
            ))}
        </GridList>
    </>
);

export const colors = () => (
    <>
        <GridList>
            {Object.values(ColorValues).map((color) => (
                <Icon key={color} {...storyDefaults} color={color} />
            ))}
        </GridList>
        <GridList>
            {Object.values(ColorValues).map((color) => (
                <Icon key={color} {...storyMaterialDefaults} color={color} />
            ))}
        </GridList>
    </>
);

export const rotation = () => (
    <GridList>
        <Icon icon="ChevronLeft" />
        <Icon icon="ChevronLeft" rotate="90" />
        <Icon icon="ChevronLeft" rotate="180" />
        <Icon icon="ChevronLeft" rotate="270" />
    </GridList>
);
