import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { AVAILABLE_ICONS } from '@components/components/Icon';
import { Pill, SUPPORTED_CONFIGURATIONS } from '@components/components/Pills/Pill';
import { PillProps } from '@components/components/Pills/types';
import { ColorValues, PillVariantValues, SizeValues, getSizeName } from '@components/theme/config';

const defaults: PillProps = {
    label: 'Label',
    size: 'md',
    variant: 'filled',
    clickable: true,
};

const meta: Meta = {
    title: 'Components / Pill',
    component: Pill,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to get pill',
        },
    },

    // Component-level argTypes
    argTypes: {
        label: {
            description: 'Label for the Pill.',
            table: {
                defaultValue: { summary: defaults.label },
            },
            control: {
                type: 'text',
            },
        },
        leftIcon: {
            description: 'The icon to display in the Pill icon.',
            type: 'string',
            options: AVAILABLE_ICONS,
            control: {
                type: 'select',
            },
        },
        rightIcon: {
            description: 'The icon to display in the Pill icon.',
            type: 'string',
            options: AVAILABLE_ICONS,
            control: {
                type: 'select',
            },
        },
        size: {
            description: 'The size of the pill.',
            options: Object.values(SizeValues),
            table: {
                defaultValue: { summary: defaults.size },
            },
            control: {
                type: 'select',
            },
        },
        variant: {
            description: 'The size of the Pill.',
            options: Object.values(PillVariantValues),
            table: {
                defaultValue: { summary: defaults.variant },
            },
            control: {
                type: 'select',
            },
        },
        color: {
            description: 'The color of the Pill.',
            options: Object.values(ColorValues),
            table: {
                defaultValue: { summary: defaults.color },
            },
            control: {
                type: 'select',
            },
        },
        showLabel: {
            description: 'Controls whether the label should be displayed.',
            table: {
                defaultValue: { summary: 'true' }, // Assuming true is the default
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Define defaults
    args: {
        label: defaults.label,
        leftIcon: defaults.leftIcon,
        rightIcon: defaults.rightIcon,
        size: defaults.size,
        variant: defaults.variant,
        showLabel: true,
    },
} satisfies Meta<typeof Pill>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Pill label={defaults.label} {...props} />,
};

export const sizes = () => (
    <GridList>
        <Pill label="Default" />
        {Object.values(SizeValues).map((size) => (
            <Pill key={size} label={getSizeName(size)} size={size} />
        ))}
    </GridList>
);

export const filled = () => (
    <GridList>
        <Pill label="Default" clickable />
        {SUPPORTED_CONFIGURATIONS[PillVariantValues.filled].map((color) => (
            <Pill key={color} label={color} color={color} clickable />
        ))}
    </GridList>
);

export const outline = () => (
    <GridList>
        <Pill label="Default" variant="outline" clickable />
        {SUPPORTED_CONFIGURATIONS[PillVariantValues.outline].map((color) => (
            <Pill key={color} label={color} color={color} variant="outline" clickable />
        ))}
    </GridList>
);

export const versionPills = () => (
    <GridList>
        {SUPPORTED_CONFIGURATIONS[PillVariantValues.version].map((color) => (
            <Pill key={color} label={color} color={color} variant="version" clickable />
        ))}
    </GridList>
);

export const withIcon = () => (
    <GridList>
        <Pill label="left icon" leftIcon="AutoMode" />
        <Pill label="right icon" rightIcon="Close" />
        <Pill label="both icon" leftIcon="AutoMode" rightIcon="Close" />
    </GridList>
);
