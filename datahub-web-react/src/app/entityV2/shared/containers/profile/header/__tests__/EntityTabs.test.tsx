import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EntityTabs } from '@app/entityV2/shared/containers/profile/header/EntityTabs';
import { EntityTab } from '@app/entityV2/shared/types';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockRouteToTab = vi.fn();
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ entityData: {}, loading: false }),
    useBaseEntity: () => ({}),
    useRouteToTab: () => mockRouteToTab,
}));

const enabled = { visible: () => true, enabled: () => true };
const summaryTab: EntityTab = { id: 'Summary', name: 'Übersicht', component: () => null, display: enabled };
const columnsTab: EntityTab = { name: 'Columns', component: () => null, display: enabled };

function renderTabs(selectedTab?: EntityTab) {
    return render(
        <CustomThemeProvider>
            <EntityTabs tabs={[summaryTab, columnsTab]} selectedTab={selectedTab} />
        </CustomThemeProvider>,
    );
}

describe('EntityTabs', () => {
    beforeEach(() => {
        mockRouteToTab.mockClear();
    });

    it('routes to the default tab by id rather than its translated name', () => {
        renderTabs();

        expect(mockRouteToTab).toHaveBeenCalledWith({ tabName: 'Summary', method: 'replace' });
    });

    it('routes by id when a tab with an id is clicked', () => {
        renderTabs(columnsTab);

        fireEvent.click(screen.getByText('Übersicht'));
        expect(mockRouteToTab).toHaveBeenLastCalledWith({ tabName: 'Summary' });
    });

    it('routes by name when a tab without an id is clicked', () => {
        renderTabs(summaryTab);

        fireEvent.click(screen.getByText('Columns'));
        expect(mockRouteToTab).toHaveBeenLastCalledWith({ tabName: 'Columns' });
    });
});
