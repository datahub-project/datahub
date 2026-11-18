import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { Button } from '@components/components/Button';
import { Popover } from '@components/components/Popover';
import { Text } from '@components/components/Text';

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

const meta = {
    title: 'Components / Popover',
    component: Popover,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle: 'A floating panel anchored to an element, for richer or interactive content than a tooltip.',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'Optional bold heading shown above the content.',
            control: {
                type: 'text',
            },
        },
        content: {
            description: 'Body of the popover. Accepts a node or a function returning one.',
            control: {
                type: 'text',
            },
        },
        placement: {
            description: 'Where the popover appears relative to its trigger.',
            options: PLACEMENTS,
            table: {
                defaultValue: { summary: 'top' },
            },
            control: {
                type: 'select',
            },
        },
        trigger: {
            description: 'Interaction(s) that open the popover.',
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
        open: {
            description: 'Controls visibility. Leave unset to let the popover manage its own state.',
            control: {
                type: 'boolean',
            },
        },
    },

    // Define defaults
    args: {
        title: 'Popover title',
        content: 'Popover content',
        placement: 'bottom',
        trigger: 'click',
        showArrow: false,
    },
} satisfies Meta<typeof Popover>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <Popover {...props}>
            <Button variant="outline">Open popover</Button>
        </Popover>
    ),
};

export const placements = () => (
    <div style={{ padding: 120, display: 'grid', gridTemplateColumns: 'repeat(3, 120px)', gap: 16 }}>
        {PLACEMENTS.map((placement) => (
            <Popover key={placement} title={placement} content="Popover content" placement={placement} showArrow>
                <Button variant="outline">{placement}</Button>
            </Popover>
        ))}
    </div>
);

export const triggers = () => (
    <GridList>
        <Popover title="Hover" content="Opens on hover or keyboard focus">
            <Button variant="outline">Hover / focus</Button>
        </Popover>
        <Popover title="Click" content="Opens on click, closes on outside click or Escape" trigger="click">
            <Button variant="outline">Click</Button>
        </Popover>
    </GridList>
);

export const contentOnly = () => (
    <Popover content="A popover without a title" trigger="click">
        <Button variant="outline">Content only</Button>
    </Popover>
);

export const interactiveContent = () => (
    <Popover
        title="Share this asset"
        trigger="click"
        placement="bottom"
        content={
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: 220 }}>
                <Text size="sm">Anyone with access to this domain can view it.</Text>
                <Button size="sm">Copy link</Button>
            </div>
        }
    >
        <Button variant="outline">Interactive content</Button>
    </Popover>
);

export const Controlled = () => {
    const [open, setOpen] = useState(false);
    return (
        <GridList>
            <Popover
                title="Controlled"
                content="Visibility is driven by parent state"
                open={open}
                onOpenChange={setOpen}
            >
                <Button variant="outline">Anchor</Button>
            </Popover>
            <Button onClick={() => setOpen((prev) => !prev)}>{open ? 'Close' : 'Open'} from outside</Button>
        </GridList>
    );
};
