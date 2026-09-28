import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { EntityTabs } from '@app/entityV2/shared/containers/profile/header/EntityTabs';
import { EntityTab } from '@app/entityV2/shared/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const tab = (name: string, routeKey?: string): EntityTab => ({
    name,
    routeKey,
    component: () => <div>{name} content</div>,
    display: { visible: () => true, enabled: () => true },
});

const TABS = [tab('Columns'), tab('Lineage'), tab('One Click Access', 'mfe-access')];

function renderTabs(selectedTab: EntityTab | undefined, routeToTab = vi.fn()) {
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <EntityContext.Provider
                    value={
                        {
                            urn: 'urn:li:dataset:1',
                            entityType: EntityType.Dataset,
                            entityData: {},
                            loading: false,
                            baseEntity: {},
                            routeToTab,
                        } as any
                    }
                >
                    <EntityTabs tabs={TABS} selectedTab={selectedTab} />
                </EntityContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
    return routeToTab;
}

/**
 * The tab row reports a tab's route key on change, and that key becomes the URL segment. Tabs without
 * one fall back to their caption, which is how every built-in tab behaves.
 */
describe('EntityTabs route keys', () => {
    it('routes to the first tab by its caption when nothing is selected', () => {
        const routeToTab = renderTabs(undefined);
        expect(routeToTab).toHaveBeenCalledWith({ tabName: 'Columns', method: 'replace' });
    });

    it('routes to a tab by its route key, not its caption, when it is clicked', () => {
        const routeToTab = renderTabs(TABS[0]);
        fireEvent.click(screen.getByText('One Click Access'));
        expect(routeToTab).toHaveBeenCalledWith({ tabName: 'mfe-access' });
    });

    it('marks a tab selected when the routed tab carries a route key', () => {
        renderTabs(TABS[2]);
        expect(screen.getByText('One Click Access content')).toBeVisible();
    });
});
