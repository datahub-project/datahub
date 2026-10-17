import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { AssertionResultsTable } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTable';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { useGetAssertionRunsQuery } from '@graphql/assertion.generated';
import { Assertion, EntityType } from '@types';

vi.mock('@graphql/assertion.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/assertion.generated')>()),
    useGetAssertionRunsQuery: vi.fn(),
}));
vi.mock(
    '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/table/AssertionResultsTableItem',
    () => ({
        AssertionResultsTableItem: ({ run }: { run: { timestampMillis: number } }) => (
            <div data-testid="run">{run.timestampMillis}</div>
        ),
    }),
);
vi.mock('@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/AssertionResultDot', () => ({
    AssertionResultDot: () => null,
}));

const assertion = { urn: 'urn:li:assertion:test', type: EntityType.Assertion } as Assertion;

function mockHistory(total: number) {
    vi.mocked(useGetAssertionRunsQuery).mockImplementation((options) => {
        const runs = Array.from({ length: total }, (_, index) => ({ timestampMillis: total - index })).slice(
            0,
            options?.variables?.limit ?? total,
        );
        return {
            data: { assertion: { runEvents: { runEvents: runs, total: runs.length } } },
            loading: false,
        } as ReturnType<typeof useGetAssertionRunsQuery>;
    });
}

describe('AssertionResultsTable pagination', () => {
    it('makes all seven results reachable when total is the returned page length', () => {
        mockHistory(7);
        render(
            <MockedProvider>
                <TestPageContainer>
                    <AssertionResultsTable assertion={assertion} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getAllByTestId('run')).toHaveLength(3);
        fireEvent.click(screen.getByText(/show more/i));
        expect(screen.getAllByTestId('run')).toHaveLength(7);
        expect(screen.queryByText(/show more/i)).not.toBeInTheDocument();
    });

    it('does not offer another page for exactly three results', () => {
        mockHistory(3);
        render(
            <MockedProvider>
                <TestPageContainer>
                    <AssertionResultsTable assertion={assertion} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getAllByTestId('run')).toHaveLength(3);
        expect(screen.queryByText(/show more/i)).not.toBeInTheDocument();
    });
});
