import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { DEFAULT_STATE, UserContext, UserContextType } from '@app/context/userContext';
import SearchFilterOptions from '@app/searchV2/filters/SearchFilterOptions';
import { UnionType } from '@app/searchV2/utils/constants';

vi.mock('@app/searchV2/filters/SaveViewButton', () => ({
    default: () => <button type="button">Save as View</button>,
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({ config: {} }),
    useIsContextDocumentsEnabled: () => false,
}));

vi.mock('@app/searchV2/filters/render/useFilterRenderer', () => ({
    useFilterRendererRegistry: () => ({
        fieldNameToRenderer: new Map(),
        hasRenderer: () => false,
        render: () => null,
    }),
}));

const ACTIVE_FILTER = { field: 'platform', values: ['hive'] };

function renderOptions(selectedViewUrn: string | null | undefined) {
    const value: UserContextType = {
        loaded: true,
        state: DEFAULT_STATE,
        localState: { selectedViewUrn },
        updateLocalState: () => null,
        updateState: () => null,
        refetchUser: () => null,
    };
    return render(
        <UserContext.Provider value={value}>
            <SearchFilterOptions
                loading={false}
                availableFilters={[]}
                activeFilters={[ACTIVE_FILTER]}
                unionType={UnionType.AND}
                onChangeFilters={() => null}
            />
        </UserContext.Provider>,
    );
}

describe('SearchFilterOptions', () => {
    it('shows Save as View after an explicit clear when filters remain', () => {
        renderOptions(null);
        expect(screen.getByRole('button', { name: 'Save as View' })).toBeInTheDocument();
    });

    it('shows Save as View when no view is stored and filters remain', () => {
        renderOptions(undefined);
        expect(screen.getByRole('button', { name: 'Save as View' })).toBeInTheDocument();
    });

    it('hides Save as View while a view is applied', () => {
        renderOptions('urn:li:dataHubView:default');
        expect(screen.queryByRole('button', { name: 'Save as View' })).not.toBeInTheDocument();
    });
});
