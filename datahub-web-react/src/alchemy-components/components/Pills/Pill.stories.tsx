import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { Star } from '@phosphor-icons/react/dist/csr/Star';
import { X } from '@phosphor-icons/react/dist/csr/X';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { Pill, SUPPORTED_CONFIGURATIONS } from '@components/components/Pills/Pill';
import { PillProps } from '@components/components/Pills/types';
import { ColorValues, PillVariantValues, SizeValues, getSizeName } from '@components/theme/config';

const ICON_OPTIONS = {
    Info,
    Star,
    Globe,
} as const;

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
            options: Object.keys(ICON_OPTIONS),
            mapping: ICON_OPTIONS,
            control: {
                type: 'select',
            },
        },
        rightIcon: {
            description: 'The icon to display in the Pill icon.',
            options: Object.keys(ICON_OPTIONS),
            mapping: ICON_OPTIONS,
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
            options: Object.values(ColorValues).filter((color) => color !== ColorValues.black),
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

export const squareFilled = () => (
    <GridList>
        <Pill label="Default" variant="squareFilled" clickable />
        {SUPPORTED_CONFIGURATIONS[PillVariantValues.squareFilled].map((color) => (
            <Pill key={color} label={color} color={color} variant="squareFilled" clickable />
        ))}
    </GridList>
);

export const squareOutline = () => (
    <GridList>
        <Pill label="Default" variant="squareOutline" clickable />
        {SUPPORTED_CONFIGURATIONS[PillVariantValues.squareOutline].map((color) => (
            <Pill key={color} label={color} color={color} variant="squareOutline" clickable />
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
        <Pill label="left icon" leftIcon={Star} />
        <Pill label="right icon" rightIcon={X} />
        <Pill label="both icon" leftIcon={Star} rightIcon={X} />
    </GridList>
);
