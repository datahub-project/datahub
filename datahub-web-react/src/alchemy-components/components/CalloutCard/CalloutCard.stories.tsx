import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Lightbulb } from '@phosphor-icons/react/dist/csr/Lightbulb';
import type { Meta, StoryObj } from '@storybook/react';
import { fn } from '@storybook/test';
import React from 'react';
import styled from 'styled-components';

import { CalloutCard } from '@components/components/CalloutCard';
import { Icon } from '@components/components/Icon';

// The fixed positions are relative to the viewport; the transform makes this frame their containing
// block so the card stays inside the story canvas.
const Frame = styled.div`
    position: relative;
    width: 600px;
    height: 260px;
    transform: translateZ(0);
`;

const meta = {
    title: 'Components / CalloutCard',
    component: CalloutCard,

    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle:
                'A standalone card for tips, announcements and guided tours, shown inline or at a fixed position.',
        },
    },

    argTypes: {
        title: {
            description: 'Title text.',
            control: { type: 'text' },
        },
        children: {
            description: 'Body content.',
            control: { type: 'text' },
        },
        icon: {
            description: 'Icon shown next to the title.',
            control: false,
        },
        position: {
            description: 'Where the card is placed.',
            options: ['inline', 'fixed-top-right', 'fixed-bottom-center'],
            control: { type: 'select' },
            table: { defaultValue: { summary: 'inline' } },
        },
        size: {
            description: 'Width range of the card.',
            options: ['sm', 'md'],
            control: { type: 'select' },
            table: { defaultValue: { summary: 'sm' } },
        },
        primaryButtonText: {
            description: 'Label of the primary button.',
            control: { type: 'text' },
        },
        showCloseButton: {
            description: 'Show a Close button in the footer (requires `onClose`).',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        showCloseButtonInHeader: {
            description: 'Show a close icon in the header (requires `onClose`).',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        onPrimaryClick: { control: false },
        onClose: { control: false },
    },

    args: {
        icon: <Icon icon={Lightbulb} size="xl" color="primary" />,
        title: 'Try the new lineage view',
        children: 'Explore upstream and downstream dependencies with column-level detail.',
        primaryButtonText: 'Got it',
        onPrimaryClick: fn(),
        onClose: fn(),
    },

    decorators: [
        (Story) => (
            <Frame>
                <Story />
            </Frame>
        ),
    ],
} satisfies Meta<typeof CalloutCard>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
};

export const inline: Story = {};

export const withCloseButtons: Story = {
    args: {
        showCloseButton: true,
        showCloseButtonInHeader: true,
    },
};

export const fixedBottomCenter: Story = {
    args: {
        position: 'fixed-bottom-center',
        size: 'md',
    },
};
