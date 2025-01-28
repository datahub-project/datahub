import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { GridList } from '@src/alchemy-components/.docs/mdx-components';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Loader, loaderDefault } from './Loader';

const meta = {
    title: 'Components / Loader',
    component: Loader,

    // Display Properties
    parameters: {
        // layout: 'flex',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to show the loading spinner',
        },
    },

    // Component-level argTypes
    argTypes: {
        size: {
            description: 'The size of the Loader',
            type: 'string',
            options: ['xs', 'sm', 'md', 'lg', 'xl'],
            table: {
                defaultValue: { summary: loaderDefault.size },
            },
            control: {
                type: 'select',
            },
        },
        justifyContent: {
            description: 'The option to configure css-option: justify-content',
            type: 'string',
            options: ['center', 'flex-start'],
            table: {
                defaultValue: { summary: loaderDefault.justifyContent },
            },
            control: {
                type: 'select',
            },
        },
        alignItems: {
            description: 'The option to configure css-option: align-items',
            type: 'string',
            options: ['center', 'flex-start', 'none'],
            table: {
                defaultValue: { summary: loaderDefault.alignItems },
            },
            control: {
                type: 'select',
            },
        },
    },

    // Define defaults
    args: loaderDefault,
} satisfies Meta<typeof Loader>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    args: {
        justifyContent: 'flex-start',
    },

    tags: ['dev'],
    render: (props) => <Loader {...props} />,
};

export const sizes = () => (
    <GridList>
        <Loader size="xs" />
        <Loader size="sm" />
        <Loader size="md" />
        <Loader size="lg" />
        <Loader size="xl" />
    </GridList>
);
