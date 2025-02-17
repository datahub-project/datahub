import React from 'react';

import type { Meta, StoryObj, StoryFn } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import { VerticalFlexGrid } from '@components/.docs/mdx-components';
import { Text, textDefaults } from '.';

// Auto Docs
const meta = {
    title: 'Typography / Text',
    component: Text,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render text and paragraphs within an interface.',
        },
    },

    // Component-level argTypes
    argTypes: {
        children: {
            description: 'The content to display within the heading.',
            table: {
                type: { summary: 'string' },
            },
        },
        type: {
            description: 'The type of text to display.',
            table: {
                defaultValue: { summary: textDefaults.type },
            },
        },
        size: {
            description: 'Override the size of the text.',
            table: {
                defaultValue: { summary: `${textDefaults.size}` },
            },
        },
        color: {
            description: 'Override the color of the text.',
            table: {
                defaultValue: { summary: textDefaults.color },
            },
        },
        weight: {
            description: 'Override the weight of the heading.',
            table: {
                defaultValue: { summary: textDefaults.weight },
            },
        },
    },

    // Define default args
    args: {
        children:
            'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas aliquet nulla id felis vehicula, et posuere dui dapibus. Nullam rhoncus massa non tortor convallis, in blandit turpis rutrum. Morbi tempus velit mauris, at mattis metus mattis sed. Nunc molestie efficitur lectus, vel mollis eros.',
        type: textDefaults.type,
        size: textDefaults.size,
        color: textDefaults.color,
        weight: textDefaults.weight,
    },
} satisfies Meta<typeof Text>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Text {...props}>{props.children}</Text>,
};

export const sizes: StoryFn<Story> = (props: any) => (
    <VerticalFlexGrid>
        <Text size="4xl">{props.children}</Text>
        <Text size="3xl">{props.children}</Text>
        <Text size="2xl">{props.children}</Text>
        <Text size="xl">{props.children}</Text>
        <Text size="lg">{props.children}</Text>
        <Text size="md">{props.children}</Text>
        <Text size="sm">{props.children}</Text>
        <Text size="xs">{props.children}</Text>
    </VerticalFlexGrid>
);

export const withLink = () => (
    <Text>
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas aliquet nulla id felis vehicula, et posuere
        dui dapibus. <a href="/">Nullam rhoncus massa non tortor convallis</a>, in blandit turpis rutrum. Morbi tempus
        velit mauris, at mattis metus mattis sed. Nunc molestie efficitur lectus, vel mollis eros.
    </Text>
);
