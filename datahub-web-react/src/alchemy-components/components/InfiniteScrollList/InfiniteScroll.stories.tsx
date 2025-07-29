import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import styled from 'styled-components';

import { InfiniteScrollList } from '@components/components/InfiniteScrollList/InfiniteScrollList';
import type { InfiniteScrollListProps } from '@components/components/InfiniteScrollList/types';

// Sample type for list items
type User = {
    id: number;
    name: string;
};

// Sample data set
const USER_DATA: User[] = Array.from({ length: 50 }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
}));

// Simulated async fetch function with pagination support
const mockFetchUsers = (start: number, count: number): Promise<User[]> => {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve(USER_DATA.slice(start, start + count));
        }, 250);
    });
};

const ScrollContainer = styled.div`
    height: 300px;
    width: 300px;
    overflow-y: auto;
    border: 1px solid #ddd;
    padding: 8px;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

// Auto Docs
const meta = {
    title: 'Lists & Tables / InfiniteScrollList',
    component: InfiniteScrollList,
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to render an infinite scroll list',
        },
    },
    argTypes: {
        fetchData: {
            description: 'Function to asynchronously fetch page data, given start index and count',
            control: false,
        },
        renderItem: {
            description: 'Render function for each list item',
            control: false,
        },
        pageSize: {
            description: 'Number of items to fetch per page',
            control: { type: 'number' },
            defaultValue: 10,
        },
        totalItemCount: {
            description: 'Optional total number of items available',
            control: { type: 'number', min: 0 },
        },
        emptyState: {
            description: 'Component/UI to show when no items are returned',
            control: false,
        },
    },
    args: {
        pageSize: 10,
        totalItemCount: USER_DATA.length,
        fetchData: mockFetchUsers,
        renderItem: (user: User) => (
            <div key={user.id} style={{ padding: 8, borderBottom: '1px solid #eee' }}>
                {user.name}
            </div>
        ),
        emptyState: <div>No users found.</div>,
    },
} satisfies Meta<InfiniteScrollListProps<User>>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],

    render: (args) => (
        <ScrollContainer>
            <InfiniteScrollList {...args} />
        </ScrollContainer>
    ),
};

export const emptyData: Story = {
    args: {
        fetchData: () => Promise.resolve([]),
        totalItemCount: 0,
    },
    render: (args) => (
        <ScrollContainer>
            <InfiniteScrollList {...args} />
        </ScrollContainer>
    ),
};

export const smallPageSize: Story = {
    args: {
        pageSize: 3,
    },
    render: (args) => (
        <ScrollContainer>
            <InfiniteScrollList {...args} />
        </ScrollContainer>
    ),
};

export const customRenderItem: Story = {
    render: (args) => (
        <ScrollContainer>
            <InfiniteScrollList
                {...args}
                renderItem={(user) => (
                    <div key={user.id} style={{ padding: 8, borderBottom: '1px solid #ccc', fontWeight: 'bold' }}>
                        {user.name}
                    </div>
                )}
            />
        </ScrollContainer>
    ),
};
