import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';

import { Link, linkDefaults } from '.';

const meta = {
    title: 'Typography / Link',
    component: Link,

    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle: 'A styled link component that wraps an anchor tag with DataHub styles.',
        },
    },

    argTypes: {
        children: {
            description: 'The content of the Link.',
            control: {
                type: 'text',
            },
        },
        href: {
            description: 'The URL that the hyperlink points to.',
            control: {
                type: 'text',
            },
        },
        color: {
            description: 'The color of the Link.',
            options: ['primary', 'violet', 'green', 'red', 'blue', 'gray'],
            table: {
                defaultValue: { summary: linkDefaults.color },
            },
            control: {
                type: 'select',
            },
        },
        target: {
            description: 'Where to open the linked document.',
            table: {
                defaultValue: { summary: linkDefaults.target },
            },
            control: {
                type: 'text',
            },
        },
        rel: {
            description: 'The relationship between the current document and the linked document.',
            table: {
                defaultValue: { summary: linkDefaults.rel },
            },
            control: {
                type: 'text',
            },
        },
    },

    args: {
        children: 'Link Text',
        href: 'https://datahub.io',
        color: linkDefaults.color,
        target: linkDefaults.target,
        rel: linkDefaults.rel,
    },
} satisfies Meta<typeof Link>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Link {...props} />,
};

export const colors = () => (
    <GridList>
        <Link href="https://datahub.io">Primary Link (Default)</Link>
        <Link href="https://datahub.io" color="violet">
            Violet Link
        </Link>
        <Link href="https://datahub.io" color="green">
            Green Link
        </Link>
        <Link href="https://datahub.io" color="red">
            Red Link
        </Link>
        <Link href="https://datahub.io" color="blue">
            Blue Link
        </Link>
        <Link href="https://datahub.io" color="gray">
            Gray Link
        </Link>
    </GridList>
);

export const inText = () => (
    <p>
        This is a paragraph with an <Link href="https://datahub.io">inline link</Link> that opens in a new tab by
        default.
    </p>
);

export const sameTab = () => (
    <Link href="https://datahub.io" target="_self">
        Link that opens in the same tab
    </Link>
);
