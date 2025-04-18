import React from 'react';

import type { Meta, StoryObj, StoryFn } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import { VerticalFlexGrid } from '@components/.docs/mdx-components';
import { Heading, headingDefaults } from '.';

// Auto Docs
const meta = {
    title: 'Typography / Heading',
    component: Heading,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render semantic HTML heading elements.',
        },
    },

    // Component-level argTypes
    argTypes: {
        children: {
            description: 'The content to display within the heading.',
            table: {
                type: { summary: 'string' },
            },
            control: {
                type: 'text',
            },
        },
        type: {
            description: 'The type of heading to display.',
            table: {
                defaultValue: { summary: headingDefaults.type },
            },
        },
        size: {
            description: 'Override the size of the heading.',
            table: {
                defaultValue: { summary: `${headingDefaults.size}` },
            },
        },
        color: {
            description: 'Override the color of the heading.',
            table: {
                defaultValue: { summary: headingDefaults.color },
            },
        },
        weight: {
            description: 'Override the weight of the heading.',
            table: {
                defaultValue: { summary: `${headingDefaults.weight}` },
            },
        },
    },

    // Define defaults
    args: {
        children: 'The content to display within the heading.',
        type: headingDefaults.type,
        size: headingDefaults.size,
        color: headingDefaults.color,
        weight: headingDefaults.weight,
    },
} satisfies Meta<typeof Heading>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Heading {...props}>{props.children}</Heading>,
};

export const sizes: StoryFn<Story> = (props: any) => (
    <VerticalFlexGrid>
        <Heading type="h1">H1 {props.children}</Heading>
        <Heading type="h2">H2 {props.children}</Heading>
        <Heading type="h3">H3 {props.children}</Heading>
        <Heading type="h4">H4 {props.children}</Heading>
        <Heading type="h5">H5 {props.children}</Heading>
        <Heading type="h6">H6 {props.children}</Heading>
    </VerticalFlexGrid>
);

export const withLink = () => (
    <Heading>
        <a href="/">The content to display within the heading</a>
    </Heading>
);
