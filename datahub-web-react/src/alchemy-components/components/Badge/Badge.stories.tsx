import React from 'react';

import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';

import { GridList } from '@components/.docs/mdx-components';
import { Badge, badgeDefault } from './Badge';
import pillMeta from '../Pills/Pill.stories';
import { omitKeys } from './utils';

const pillMetaArgTypes = omitKeys(pillMeta.argTypes, ['label']);
const pillMetaArgs = omitKeys(pillMeta.args, ['label']);

const meta = {
    title: 'Components / Badge',
    component: Badge,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to get badge',
        },
    },

    // Component-level argTypes
    argTypes: {
        count: {
            description: 'Count to show.',
            table: {
                defaultValue: { summary: `${badgeDefault.count}` },
            },
            control: {
                type: 'number',
            },
        },
        overflowCount: {
            description: 'Max count to show.',
            table: {
                defaultValue: { summary: `${badgeDefault.overflowCount}` },
            },
            control: {
                type: 'number',
            },
        },
        showZero: {
            description: 'Whether to show badge when `count` is zero.',
            table: {
                defaultValue: { summary: `${badgeDefault.showZero}` },
            },
            control: {
                type: 'boolean',
            },
        },
        ...pillMetaArgTypes,
    },

    // Define defaults
    args: {
        count: 100,
        overflowCount: badgeDefault.overflowCount,
        showZero: badgeDefault.showZero,
        ...pillMetaArgs,
    },
} satisfies Meta<typeof Badge>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Badge {...props} />,
};

export const sizes = () => (
    <GridList>
        <Badge count={100} />
        <Badge count={100} size="sm" />
        <Badge count={100} size="lg" />
    </GridList>
);

export const colors = () => (
    <GridList>
        <Badge count={100} />
        <Badge count={100} color="violet" />
        <Badge count={100} color="green" />
        <Badge count={100} color="red" />
        <Badge count={100} color="blue" />
        <Badge count={100} color="gray" />
    </GridList>
);

export const withIcon = () => (
    <GridList>
        <Badge count={100} leftIcon="AutoMode" />
        <Badge count={100} rightIcon="Close" />
        <Badge count={100} leftIcon="AutoMode" rightIcon="Close" />
    </GridList>
);
