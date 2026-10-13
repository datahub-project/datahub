import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { DEFAULT_STATE, UserContext, UserContextType } from '@app/context/userContext';
import EmptySearchResults from '@app/searchV2/EmptySearchResults';
import { isSearchViewReady } from '@app/searchV2/useIsSearchViewReady';
import themeV2 from '@conf/theme/themeV2';

const STORED_VIEW_URN = 'urn:li:dataHubView:default';

vi.mock('@app/searchV2/useGetSearchQueryInputs', () => ({
    default: () => ({
        query: 'events',
        filters: [],
        viewUrn: STORED_VIEW_URN,
    }),
}));

describe('EmptySearchResults', () => {
    it('stores an explicit view clear as null so search stays ready', () => {
        const updateLocalState = vi.fn();
        const value: UserContextType = {
            loaded: true,
            state: DEFAULT_STATE,
            localState: { selectedViewUrn: STORED_VIEW_URN },
            updateLocalState,
            updateState: () => null,
            refetchUser: () => null,
        };

        render(
            <ThemeProvider theme={themeV2}>
                <MemoryRouter>
                    <UserContext.Provider value={value}>
                        <EmptySearchResults suggestions={[]} />
                    </UserContext.Provider>
                </MemoryRouter>
            </ThemeProvider>,
        );

        fireEvent.click(screen.getByText('clearing the selected view'));

        expect(updateLocalState).toHaveBeenCalledWith({ selectedViewUrn: null });
        expect(isSearchViewReady(false, updateLocalState.mock.calls[0][0].selectedViewUrn)).toBe(true);
    });
});
