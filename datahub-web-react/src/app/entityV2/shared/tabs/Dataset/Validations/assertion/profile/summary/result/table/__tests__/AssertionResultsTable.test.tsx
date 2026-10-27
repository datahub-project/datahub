import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AssertionResultsTable } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTable';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { useGetAssertionRunsQuery } from '@graphql/assertion.generated';
import { Assertion, AssertionResultType, AssertionRunEvent, EntityType } from '@types';

vi.mock('@graphql/assertion.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/assertion.generated')>()),
    useGetAssertionRunsQuery: vi.fn(),
}));

vi.mock(
    '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTableItem',
    () => ({
        AssertionResultsTableItem: ({ run }: { run: AssertionRunEvent }) => (
            <div data-testid="assertion-run">{run.timestampMillis}</div>
        ),
    }),
);

const ASSERTION = { urn: 'urn:li:assertion:test', type: EntityType.Assertion } as Assertion;

const makeRun = (index: number) => ({
    timestampMillis: 1_000_000 - index,
    result: { type: AssertionResultType.Success },
});

// Emulates AssertionRunEventResolver: returns up to `limit` events and reports `total` as the page size.
const mockRunsAvailable = (available: number) => {
    vi.mocked(useGetAssertionRunsQuery).mockImplementation((options) => {
        const limit = options?.variables?.limit ?? available;
        const runEvents = Array.from({ length: Math.min(limit, available) }, (_, i) => makeRun(i));
        return {
            data: { assertion: { runEvents: { total: runEvents.length, runEvents } } },
            previousData: undefined,
            loading: false,
        } as unknown as ReturnType<typeof useGetAssertionRunsQuery>;
    });
};

const renderTable = () =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <AssertionResultsTable assertion={ASSERTION} />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('AssertionResultsTable', () => {
    beforeEach(() => {
        vi.mocked(useGetAssertionRunsQuery).mockReset();
    });

    it('paginates through all runs when more exist than the initial page', () => {
        mockRunsAvailable(7);
        renderTable();

        expect(screen.getAllByTestId('assertion-run')).toHaveLength(3);
        expect(vi.mocked(useGetAssertionRunsQuery).mock.lastCall?.[0]?.variables?.limit).toBe(4);

        fireEvent.click(screen.getByText('Show more'));

        expect(screen.getAllByTestId('assertion-run')).toHaveLength(7);
        expect(screen.queryByText('Show more')).not.toBeInTheDocument();
    });

    it('does not offer another page when exactly the initial count of runs exists', () => {
        mockRunsAvailable(3);
        renderTable();

        expect(screen.getAllByTestId('assertion-run')).toHaveLength(3);
        expect(screen.queryByText('Show more')).not.toBeInTheDocument();
    });
});
