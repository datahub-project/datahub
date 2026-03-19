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
    border: 1px solid ${(props) => props.theme.colors.border};
    padding: 8px;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const UserRow = styled.div`
    padding: 8px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    width: 100%;
    box-sizing: border-box;
`;

const UserRowBold = styled(UserRow)`
    font-weight: bold;
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
            renderItem: (user: User) => <UserRow key={user.id}>{user.name}</UserRow>,
            control: false,
        },
    },
    args: {
        pageSize: 10,
        totalItemCount: USER_DATA.length,
        fetchData: mockFetchUsers,
        renderItem: (user: User) => <UserRow key={user.id}>{user.name}</UserRow>,
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
export const customRenderItem: Story = {
    render: (args) => (
        <ScrollContainer>
            <InfiniteScrollList {...args} renderItem={(user) => <UserRowBold key={user.id}>{user.name}</UserRowBold>} />
        </ScrollContainer>
    ),
};
