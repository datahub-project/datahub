import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { EntitySidebarTabKey } from '@app/entityV2/shared/constants';
import { EntitySidebarTabs } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarTabs';
import { EntitySidebarTab } from '@app/entityV2/shared/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const TabIcon = () => null;
const TabContent = () => null;

function makeTab(key: string, name: string): EntitySidebarTab {
    return { key, name, icon: TabIcon, component: TabContent };
}

function renderTabs(tabs: EntitySidebarTab[]) {
    return render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <EntitySidebarTabs tabs={tabs} selectedTab={tabs[0]} onSelectTab={() => {}} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('EntitySidebarTabs', () => {
    it('picks the short tab label by key, not by the translated tab name', () => {
        // Names as in a non-English UI: they no longer match the English tab names
        renderTabs([
            makeTab(EntitySidebarTabKey.SUMMARY, 'Übersicht'),
            makeTab(EntitySidebarTabKey.PROPERTIES, 'Eigenschaften'),
        ]);

        expect(screen.getByText('Props')).toBeInTheDocument();
        expect(screen.queryByText('Eigenschaften')).not.toBeInTheDocument();
    });

    it('shows the tab name for a tab without a short label', () => {
        renderTabs([makeTab(EntitySidebarTabKey.ABOUT, 'Om')]);

        expect(screen.getByText('Om')).toBeInTheDocument();
    });
});
