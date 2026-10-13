import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import { describe, expect, it, vi } from 'vitest';

import { DEFAULT_STATE, UserContext, UserContextType } from '@app/context/userContext';
import ViewSelectContextProvider, { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import { isSearchViewReady } from '@app/searchV2/useIsSearchViewReady';

const STORED_VIEW_URN = 'urn:li:dataHubView:default';

vi.mock('@graphql/view.generated', () => ({
    useListMyViewsQuery: () => ({ data: undefined }),
    useListGlobalViewsQuery: () => ({ data: undefined }),
}));

function ClearViewButton() {
    const { onClear } = useViewsSelectContext();
    return (
        <button type="button" onClick={onClear}>
            clear view
        </button>
    );
}

describe('ViewSelectContext', () => {
    it('stores the view pill clear as null so search stays ready', () => {
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
            <MemoryRouter>
                <UserContext.Provider value={value}>
                    <ViewSelectContextProvider>
                        <ClearViewButton />
                    </ViewSelectContextProvider>
                </UserContext.Provider>
            </MemoryRouter>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'clear view' }));

        expect(updateLocalState).toHaveBeenCalledWith({ selectedViewUrn: null });
        expect(isSearchViewReady(false, updateLocalState.mock.calls[0][0].selectedViewUrn)).toBe(true);
    });
});
