import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useEffect, useState } from 'react';

import { Pagination } from '@components/components/Pagination/Pagination';
import { PaginationProps, paginationDefaults } from '@components/components/Pagination/types';

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
        total: {
            description: 'Total number of items across all pages',
            table: {
                defaultValue: { summary: `${paginationDefaults.total}` },
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
        total: paginationDefaults.total,
        loading: paginationDefaults.loading,
    },
} satisfies Meta<typeof Pagination>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
const PaginationWithState = (args: PaginationProps) => {
    const [page, setPage] = useState(args.currentPage);
    const [pageSize, setPageSize] = useState(args.itemsPerPage);
    useEffect(() => setPage(args.currentPage), [args.currentPage]);
    useEffect(() => setPageSize(args.itemsPerPage), [args.itemsPerPage]);
    return (
        <Pagination
            {...args}
            currentPage={page}
            itemsPerPage={pageSize}
            onPageChange={(nextPage, nextPageSize) => {
                setPage(nextPage);
                setPageSize(nextPageSize);
                args.onPageChange?.(nextPage, nextPageSize);
            }}
        />
    );
};

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <PaginationWithState {...props} />,
};

export const WithSizeChanger = () => (
    <PaginationWithState currentPage={3} itemsPerPage={10} total={500} showSizeChanger />
);
