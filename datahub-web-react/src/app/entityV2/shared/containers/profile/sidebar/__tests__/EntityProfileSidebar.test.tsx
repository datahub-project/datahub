import { MockedProvider } from '@apollo/client/testing';
import { ListBullets } from '@phosphor-icons/react/dist/csr/ListBullets';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { EntitySidebarTabKey } from '@app/entityV2/shared/constants';
import EntityProfileSidebar from '@app/entityV2/shared/containers/profile/sidebar/EntityProfileSidebar';
import { EntitySidebarTab, TabContextType } from '@app/entityV2/shared/types';
import CompactContext from '@app/sharedV2/CompactContext';
import EntitySidebarContext, { entitySidebarContextDefaults } from '@app/sharedV2/EntitySidebarContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const WIDTH = 400;

const tabs: EntitySidebarTab[] = [
    {
        key: EntitySidebarTabKey.SUMMARY,
        name: 'Summary',
        component: () => <div>summary</div>,
        icon: ListBullets,
        display: { visible: () => true, enabled: () => true },
    },
];

function renderSidebar({ isCompact, contextType }: { isCompact: boolean; contextType?: TabContextType }) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <EntitySidebarContext.Provider
                    value={{ ...entitySidebarContextDefaults, width: WIDTH, setWidth: vi.fn() }}
                >
                    <CompactContext.Provider value={isCompact}>
                        <EntityProfileSidebar tabs={tabs} type="card" width={WIDTH} contextType={contextType} />
                    </CompactContext.Provider>
                </EntitySidebarContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
    return screen.getByTestId('entity-profile-sidebar');
}

describe('EntityProfileSidebar width', () => {
    // Compact hosts (search results, lineage, task center) wrap the sidebar in a shrink-to-fit
    // container. A percentage cap there is cyclic and lets the wrapper swallow the whole row, which
    // hid the search results when a document was the highlighted result.
    it('pins the exact width in a compact host even with the default profile context type', () => {
        const sidebar = renderSidebar({ isCompact: true });
        expect(sidebar).toHaveStyle({ minWidth: `${WIDTH}px`, maxWidth: `${WIDTH}px` });
    });

    it('pins the exact width in the search sidebar context', () => {
        const sidebar = renderSidebar({ isCompact: true, contextType: TabContextType.SEARCH_SIDEBAR });
        expect(sidebar).toHaveStyle({ minWidth: `${WIDTH}px`, maxWidth: `${WIDTH}px` });
    });

    it('caps the width to a share of the row on the entity page', () => {
        const sidebar = renderSidebar({ isCompact: false, contextType: TabContextType.PROFILE_SIDEBAR });
        expect(sidebar).not.toHaveStyle({ maxWidth: `${WIDTH}px` });
    });
});
