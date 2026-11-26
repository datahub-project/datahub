import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { Button } from '@components/components/Button';
import { Tooltip } from '@components/components/Tooltip';

const PLACEMENTS = [
    'top',
    'topLeft',
    'topRight',
    'bottom',
    'bottomLeft',
    'bottomRight',
    'left',
    'leftTop',
    'leftBottom',
    'right',
    'rightTop',
    'rightBottom',
] as const;

const LONG_TEXT =
    'This dataset holds one row per order placed through the storefront. It is refreshed hourly from the ' +
    'transactional database and backfilled nightly. Columns marked as PII are masked for anyone outside the ' +
    'data platform group. Refer to the domain documentation for details on retention, ownership, and the ' +
    'upstream jobs that feed this table. Questions should go to the owning team listed on the entity page.';

const meta = {
    title: 'Components / Tooltip',
    component: Tooltip,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle: 'Shows a short hint when the user hovers over or focuses an element.',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'Content of the tooltip. Plain text is clamped to 5 lines by default.',
            control: {
                type: 'text',
            },
        },
        placement: {
            description: 'Where the tooltip appears relative to its trigger.',
            options: PLACEMENTS,
            table: {
                defaultValue: { summary: 'top' },
            },
            control: {
                type: 'select',
            },
        },
        trigger: {
            description: 'Interaction(s) that open the tooltip.',
            options: ['hover', 'focus', 'click'],
            table: {
                defaultValue: { summary: "['hover', 'focus']" },
            },
            control: {
                type: 'select',
            },
        },
        showArrow: {
            description: 'Whether to render an arrow pointing at the trigger.',
            table: {
                defaultValue: { summary: 'false' },
            },
            control: {
                type: 'boolean',
            },
        },
        maxLines: {
            description: 'Clamps the content to this many lines. Values below 1 disable clamping.',
            table: {
                defaultValue: { summary: '5 for plain text, none otherwise' },
            },
            control: {
                type: 'number',
            },
        },
        mouseEnterDelay: {
            description: 'Delay in seconds before the tooltip opens on hover.',
            control: {
                type: 'number',
            },
        },
        open: {
            description: 'Controls visibility. Leave unset to let the tooltip manage its own state.',
            control: {
                type: 'boolean',
            },
        },
    },

    // Define defaults
    args: {
        title: 'Tooltip text',
        placement: 'top',
        showArrow: false,
    },
} satisfies Meta<typeof Tooltip>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <Tooltip {...props}>
            <Button variant="outline">Hover me</Button>
        </Tooltip>
    ),
};

export const placements = () => (
    <div style={{ padding: 80, display: 'grid', gridTemplateColumns: 'repeat(3, 120px)', gap: 16 }}>
        {PLACEMENTS.map((placement) => (
            <Tooltip key={placement} title={placement} placement={placement} showArrow>
                <Button variant="outline">{placement}</Button>
            </Tooltip>
        ))}
    </div>
);

export const triggers = () => (
    <GridList>
        <Tooltip title="Opens on hover or keyboard focus">
            <Button variant="outline">Hover / focus</Button>
        </Tooltip>
        <Tooltip title="Opens on click" trigger="click">
            <Button variant="outline">Click</Button>
        </Tooltip>
    </GridList>
);

export const withArrow = () => (
    <GridList>
        <Tooltip title="Without arrow">
            <Button variant="outline">Default</Button>
        </Tooltip>
        <Tooltip title="With arrow" showArrow>
            <Button variant="outline">Arrow</Button>
        </Tooltip>
    </GridList>
);

export const longContent = () => (
    <GridList>
        <Tooltip title={LONG_TEXT}>
            <Button variant="outline">Clamped to 5 lines</Button>
        </Tooltip>
        <Tooltip title={LONG_TEXT} maxLines={0}>
            <Button variant="outline">No clamping</Button>
        </Tooltip>
    </GridList>
);

export const richContent = () => (
    <Tooltip
        title={
            <div>
                <strong>Last updated</strong>
                <div>2 hours ago by the ingestion pipeline</div>
            </div>
        }
    >
        <Button variant="outline">Rich content</Button>
    </Tooltip>
);

// Disabled controls don't fire mouse events, so the tooltip wraps them to still explain why.
export const onDisabledButton = () => (
    <Tooltip title="You need edit permissions to do this">
        <Button variant="filled" disabled>
            Disabled
        </Button>
    </Tooltip>
);
