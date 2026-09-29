import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EntitySearchInputV2 } from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputV2';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { EntityType } from '@src/types.generated';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const dataset = (urn: string, name: string) => ({ urn, type: 'DATASET', name, properties: { name } });

const BROWSED = dataset('urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)', 'orders');
const TYPED = dataset('urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)', 'events');

// What the autocomplete request currently answers with — set per test, since the point of the guard
// is how a response relates to the query typed at the time.
let autoCompleteState: { data?: any; error?: Error } = {};

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetSearchResultsForMultipleQuery: () => ({
        data: { searchAcrossEntities: { searchResults: [{ entity: BROWSED }] } },
    }),
    useGetAutoCompleteMultipleResultsLazyQuery: () => [vi.fn(), autoCompleteState],
}));

const answeredWith = (query: string) => ({
    data: { autoCompleteForMultiple: { query, suggestions: [{ entities: [TYPED] }] } },
});

describe('EntitySearchInputV2', () => {
    beforeEach(() => {
        autoCompleteState = {};
        mockVisibilityObserver();
    });

    const renderInput = () => {
        const { rerender } = render(
            <CustomThemeProvider>
                <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                    <MemoryRouter>
                        <EntitySearchInputV2 entityTypes={[EntityType.Dataset]} placeholder="Pick a table" />
                    </MemoryRouter>
                </EntityRegistryContext.Provider>
            </CustomThemeProvider>,
        );
        fireEvent.click(screen.getByText('Pick a table'));
        return {
            // Re-render so the component picks up the response the test has just made current.
            respond: (state: typeof autoCompleteState) => {
                autoCompleteState = state;
                rerender(
                    <CustomThemeProvider>
                        <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                            <MemoryRouter>
                                <EntitySearchInputV2 entityTypes={[EntityType.Dataset]} placeholder="Pick a table" />
                            </MemoryRouter>
                        </EntityRegistryContext.Provider>
                    </CustomThemeProvider>,
                );
            },
        };
    };

    const type = (query: string) => fireEvent.change(screen.getByRole('textbox'), { target: { value: query } });

    it('browses with the wildcard results before anything is typed', () => {
        renderInput();

        expect(screen.getByText('orders')).toBeInTheDocument();
    });

    it('hides the results of an earlier query until the current one is answered', async () => {
        const { respond } = renderInput();

        type('eve');
        await waitFor(() => expect(screen.queryByText('orders')).not.toBeInTheDocument());

        respond(answeredWith('ev'));
        expect(screen.queryByText('events')).not.toBeInTheDocument();

        respond(answeredWith('eve'));
        expect(screen.getByText('events')).toBeInTheDocument();
    });

    // GMS escapes forward slashes before searching and echoes the escaped query back.
    it('accepts an answer whose echoed query came back escaped', async () => {
        const { respond } = renderInput();

        type('my_db/events');
        await waitFor(() => expect(screen.queryByText('orders')).not.toBeInTheDocument());

        respond(answeredWith('my_db\\/events'));

        expect(screen.getByText('events')).toBeInTheDocument();
    });

    it('offers nothing when the request fails, rather than waiting forever', async () => {
        const { respond } = renderInput();

        type('eve');
        respond({ error: new Error('autocomplete is down') });

        await waitFor(() => expect(screen.getByText('No results found')).toBeInTheDocument());
        expect(screen.queryByText('orders')).not.toBeInTheDocument();
    });
});
