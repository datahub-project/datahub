import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import styled from 'styled-components';

import { Divider, dividerDefault } from '@components/components/Divider/Divider';
import { Text } from '@components/components/Text';

const meta = {
    title: 'Components / Divider',
    component: Divider,

    // Display Properties
    parameters: {
        layout: 'padded',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A line that separates content horizontally or inline',
        },
    },

    // Component-level argTypes
    argTypes: {
        type: {
            description: 'The direction of the divider.',
            options: ['horizontal', 'vertical'],
            table: {
                defaultValue: { summary: dividerDefault.type },
            },
            control: {
                type: 'select',
            },
        },
        dashed: {
            description: 'Whether to draw a dashed line.',
            table: {
                defaultValue: { summary: `${dividerDefault.dashed}` },
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Define defaults
    args: dividerDefault,
} satisfies Meta<typeof Divider>;

export default meta;

type Story = StoryObj<typeof meta>;

const CompactDivider = styled(Divider)`
    margin: 8px 0;
`;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <div>
            <Text>Above</Text>
            <Divider {...props} />
            <Text>Below</Text>
        </div>
    ),
};

export const horizontal = () => (
    <div>
        <Text>Solid</Text>
        <Divider />
        <Text>Dashed</Text>
        <Divider dashed />
        <Text>Custom margin via styled()</Text>
        <CompactDivider />
        <Text>End</Text>
    </div>
);

export const vertical = () => (
    <Text>
        First
        <Divider type="vertical" />
        Second
        <Divider type="vertical" dashed />
        Third
    </Text>
);
