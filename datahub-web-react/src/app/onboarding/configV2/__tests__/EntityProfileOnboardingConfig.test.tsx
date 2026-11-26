import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { DatasetEntity } from '@app/entityV2/dataset/DatasetEntity';
import { EntitySidebarTabs } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarTabs';
import { useFinalSidebarTabs } from '@app/entityV2/shared/containers/profile/utils';
import { TabContextType } from '@app/entityV2/shared/types';
import EntityProfileOnboardingConfig, {
    ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
    ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
    ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
    ENTITY_SIDEBAR_V2_PROPERTIES_ID,
} from '@app/onboarding/configV2/EntityProfileOnboardingConfig';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const SIDEBAR_TAB_STEP_IDS = [
    ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
    ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
    ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
    ENTITY_SIDEBAR_V2_PROPERTIES_ID,
];

const SIDEBAR_SECTIONS = [{ component: () => null }];

function DatasetSidebarTabs({ renameTabs }: { renameTabs: boolean }) {
    const tabs = useFinalSidebarTabs(
        new DatasetEntity().getSidebarTabs(),
        SIDEBAR_SECTIONS,
        TabContextType.PROFILE_SIDEBAR,
    );
    // Simulates a translated UI, where tab display names differ from the English ones
    const displayedTabs = renameTabs ? tabs.map((tab) => ({ ...tab, name: `Translated ${tab.name}` })) : tabs;
    return <EntitySidebarTabs tabs={displayedTabs} selectedTab={displayedTabs[0]} onSelectTab={() => {}} />;
}

describe('EntityProfileOnboardingConfig sidebar tab steps', () => {
    it.each([
        ['default tab names', false],
        ['translated tab names', true],
    ])('every step targets a rendered dataset sidebar tab with %s', (_, renameTabs) => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <DatasetSidebarTabs renameTabs={renameTabs} />
                </TestPageContainer>
            </MockedProvider>,
        );

        SIDEBAR_TAB_STEP_IDS.forEach((stepId) => {
            const step = EntityProfileOnboardingConfig.find((s) => s.id === stepId);
            expect(step?.selector).toBeDefined();
            expect(document.querySelector(step?.selector as string)).not.toBeNull();
        });
    });
});
