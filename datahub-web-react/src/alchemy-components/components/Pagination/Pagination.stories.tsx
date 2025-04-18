import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Pagination } from './Pagination';
import { paginationDefaults } from './types';

// Auto Docs
const meta = {
    title: 'Components / Pagination',
    component: Pagination,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render pagination options.',
        },
    },

    // Component-level argTypes
    argTypes: {
        currentPage: {
            description: 'Current selected page',
            table: {
                defaultValue: { summary: `${paginationDefaults.currentPage}` },
            },
            control: {
                type: 'number',
            },
        },
        itemsPerPage: {
            description: 'Number of items shown per page',
            table: {
                defaultValue: { summary: `${paginationDefaults.itemsPerPage}` },
            },
            control: {
                type: 'number',
            },
        },
        totalPages: {
            description: 'Total number of pages',
            table: {
                defaultValue: { summary: `${paginationDefaults.totalPages}` },
            },
            control: {
                type: 'number',
            },
        },
        loading: {
            description: 'If the data is loading',
            table: {
                defaultValue: { summary: `${paginationDefaults.loading}` },
            },
            control: {
                type: 'boolean',
            },
        },
        onPageChange: {
            description: 'On change function for when the page is changed',
        },
    },

    // Define default args
    args: {
        currentPage: paginationDefaults.currentPage,
        itemsPerPage: paginationDefaults.itemsPerPage,
        totalPages: paginationDefaults.totalPages,
        loading: paginationDefaults.loading,
    },
} satisfies Meta<typeof Pagination>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Pagination {...props} />,
};

export const withSizeChanger = () => <Pagination currentPage={3} itemsPerPage={1} totalPages={10} showSizeChanger />;
