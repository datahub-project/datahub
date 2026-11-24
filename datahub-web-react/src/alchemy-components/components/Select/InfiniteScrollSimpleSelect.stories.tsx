import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { fn } from '@storybook/test';
import React, { useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { InfiniteScrollSimpleSelect } from '@components/components/Select/InfiniteScrollSimpleSelect';
import { SelectOption } from '@components/components/Select/types';

const PAGE_SIZE = 10;
const TOTAL = 45;
const FAKE_LATENCY_MS = 600;

function makePage(page: number): SelectOption[] {
    const start = page * PAGE_SIZE;
    return Array.from({ length: Math.min(PAGE_SIZE, TOTAL - start) }, (_, i) => ({
        value: `item-${start + i + 1}`,
        label: `Item ${start + i + 1}`,
    }));
}

const meta = {
    title: 'Forms / InfiniteScrollSimpleSelect',
    component: InfiniteScrollSimpleSelect,

    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle:
                'SimpleSelect that renders a scroll sentinel and loader at the end of the list so callers can fetch the next page.',
        },
    },

    argTypes: {
        options: {
            description: 'Options loaded so far.',
            control: { type: 'object' },
        },
        loading: {
            description: 'Show a loader row after the current options while the next page loads.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        hasMore: {
            description: 'Render the scroll sentinel row; when it becomes visible, load the next page.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        scrollRef: {
            description:
                'Ref (or callback ref, e.g. from `useInView`) attached to the sentinel row at the bottom of the list.',
            control: false,
        },
    },

    args: {
        options: makePage(0),
        label: 'Paginated options',
        loading: false,
        hasMore: true,
        onUpdate: fn(),
    },
} satisfies Meta<typeof InfiniteScrollSimpleSelect>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (args) => <InfiniteScrollSimpleSelect {...args} />,
};

const PaginatedDemo = () => {
    const [options, setOptions] = useState<SelectOption[]>(makePage(0));
    const [loading, setLoading] = useState(false);
    const { ref, inView } = useInView();
    const hasMore = options.length < TOTAL;

    useEffect(() => {
        if (!inView || loading || !hasMore) return;
        setLoading(true);
        // No cleanup: clearing on the `loading` change would cancel this fetch.
        setTimeout(() => {
            setOptions((prev) => [...prev, ...makePage(prev.length / PAGE_SIZE)]);
            setLoading(false);
        }, FAKE_LATENCY_MS);
    }, [inView, loading, hasMore]);

    return (
        <InfiniteScrollSimpleSelect
            options={options}
            label={`Loaded ${options.length} of ${TOTAL}`}
            loading={loading}
            hasMore={hasMore}
            scrollRef={ref}
            width={255}
        />
    );
};

export const infiniteScroll: Story = {
    render: () => <PaginatedDemo />,
};
