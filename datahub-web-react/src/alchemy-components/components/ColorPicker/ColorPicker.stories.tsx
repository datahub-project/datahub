import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { fn } from '@storybook/test';
import React, { useState } from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { ColorPicker } from '@components/components/ColorPicker';
import { Text } from '@components/components/Text';

const meta = {
    title: 'Forms / ColorPicker',
    component: ColorPicker,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle: 'Lets the user pick a color from preset swatches or enter a hex code.',
        },
    },

    // Component-level argTypes
    argTypes: {
        initialColor: {
            description: 'Starting hex color. Falls back to the theme default.',
            control: {
                type: 'color',
            },
        },
        label: {
            description: 'Label shown above the picker.',
            control: {
                type: 'text',
            },
        },
        showDots: {
            description: 'Whether to show the preset color swatches.',
            table: {
                defaultValue: { summary: 'true' },
            },
            control: {
                type: 'boolean',
            },
        },
        onChange: {
            description: 'Called with the committed hex value, or an empty string when cleared.',
        },
    },

    // Define defaults
    args: {
        label: 'Color',
        showDots: true,
        onChange: fn(),
    },
} satisfies Meta<typeof ColorPicker>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div style={{ width: 320 }}>
            <ColorPicker {...props} />
        </div>
    ),
};

export const variants = () => (
    <GridList width="700px">
        <div style={{ width: 320 }}>
            <ColorPicker label="With swatches" onChange={fn()} />
        </div>
        <div style={{ width: 320 }}>
            <ColorPicker label="Hex only" showDots={false} initialColor="#1890ff" onChange={fn()} />
        </div>
    </GridList>
);

export const WithValue = () => {
    const [value, setValue] = useState('#533fd1');
    return (
        <div style={{ width: 320, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <ColorPicker label="Brand color" initialColor="#533fd1" onChange={setValue} />
            <Text size="sm">Committed value: {value || '(cleared)'}</Text>
        </div>
    );
};
