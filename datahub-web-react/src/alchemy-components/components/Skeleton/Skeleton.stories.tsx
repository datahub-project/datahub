import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import styled from 'styled-components';

import { GridList } from '@components/.docs/mdx-components';
import { Skeleton, skeletonDefault } from '@components/components/Skeleton/Skeleton';

const meta = {
    title: 'Components / Skeleton',
    component: Skeleton,

    // Display Properties
    parameters: {
        layout: 'padded',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'Placeholder shapes shown while content is loading',
        },
    },

    // Component-level argTypes
    argTypes: {
        active: {
            description: 'Whether to show the shimmer animation.',
            table: {
                defaultValue: { summary: `${skeletonDefault.active}` },
            },
            control: {
                type: 'boolean',
            },
        },
        title: {
            description: 'Show a title bar, or `{ width }` to size it.',
            table: {
                defaultValue: { summary: `${skeletonDefault.title}` },
            },
            control: {
                type: 'object',
            },
        },
        paragraph: {
            description: 'Show paragraph rows, or `{ rows, width }` to configure them.',
            table: {
                defaultValue: { summary: `${skeletonDefault.paragraph}` },
            },
            control: {
                type: 'object',
            },
        },
    },

    // Define defaults
    args: skeletonDefault,
} satisfies Meta<typeof Skeleton>;

export default meta;

type Story = StoryObj<typeof meta>;

const StyledCardSkeleton = styled(Skeleton.Button)`
    width: 240px;
    height: 80px;
    border-radius: 8px;
`;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Skeleton {...props} />,
};

export const paragraph = () => (
    <GridList isVertical>
        <Skeleton />
        <Skeleton title={false} paragraph={{ rows: 4 }} />
        <Skeleton paragraph={{ rows: 2, width: ['80%', 120] }} />
        <Skeleton active={false} />
    </GridList>
);

export const buttons = () => (
    <GridList>
        <Skeleton.Button size="sm" />
        <Skeleton.Button />
        <Skeleton.Button size="lg" />
        <Skeleton.Button shape="round" />
        <Skeleton.Button shape="circle" />
        <Skeleton.Button shape="square" width="8rem" />
    </GridList>
);

export const inputs = () => (
    <GridList isVertical>
        <Skeleton.Input size="sm" />
        <Skeleton.Input />
        <Skeleton.Input size="lg" />
        <Skeleton.Input block />
    </GridList>
);

export const avatars = () => (
    <GridList>
        <Skeleton.Avatar size="sm" />
        <Skeleton.Avatar />
        <Skeleton.Avatar size="lg" />
        <Skeleton.Avatar size={56} />
        <Skeleton.Avatar shape="square" />
    </GridList>
);

export const styledOverride = () => <StyledCardSkeleton />;
