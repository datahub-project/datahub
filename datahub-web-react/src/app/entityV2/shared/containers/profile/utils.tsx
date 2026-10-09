import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import isEqual from 'lodash/isEqual';
import React, { useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { GLOSSARY_ENTITY_TYPES } from '@app/entityV2/shared/constants';
import { ENTITY_TAB_NAME_REGEX_PATTERN } from '@app/entityV2/shared/containers/profile/entityData';
import EntitySidebarSectionsTab from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebarSectionsTab';
import SidebarPopularityHeaderSection from '@app/entityV2/shared/containers/profile/sidebar/shared/SidebarPopularityHeaderSection';
import {
    PopularityTier,
    getBarsStatusFromPopularityTier,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { useExtraSidebarTabs } from '@app/entityV2/shared/containers/profile/useExtraSidebarTabs';
import { EntitySidebarSection, EntitySidebarTab, EntityTab, TabContextType } from '@app/entityV2/shared/types';
import {
    ENTITY_PROFILE_DOMAINS_ID,
    ENTITY_PROFILE_GLOSSARY_TERMS_ID,
    ENTITY_PROFILE_LINEAGE_ID,
    ENTITY_PROFILE_OWNERS_ID,
    ENTITY_PROFILE_PROPERTIES_ID,
    ENTITY_PROFILE_TAGS_ID,
    ENTITY_PROFILE_V2_SIDEBAR_ID,
} from '@app/onboarding/config/EntityProfileOnboardingConfig';
import {
    ENTITY_PROFILE_V2_COLUMNS_ID,
    ENTITY_PROFILE_V2_CONTENTS_ID,
    ENTITY_PROFILE_V2_DOCUMENTATION_ID,
    ENTITY_PROFILE_V2_INCIDENTS_ID,
    ENTITY_PROFILE_V2_QUERIES_ID,
    ENTITY_PROFILE_V2_VALIDATION_ID,
    ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
    ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
    ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
    ENTITY_SIDEBAR_V2_PROPERTIES_ID,
} from '@app/onboarding/configV2/EntityProfileOnboardingConfig';
import usePrevious from '@app/shared/usePrevious';

import { EntityType } from '@types';

export type SidebarStatsColumn = {
    title: React.ReactNode;
    content: React.ReactNode;
};

export {
    getDataForEntityType,
    getEntityPath,
    useEntityQueryParams,
    useGlossaryActiveTabPath,
} from '@app/entityV2/shared/containers/profile/entityData';

export function useRoutedTab(tabs: EntityTab[]): EntityTab | undefined {
    const { pathname } = useLocation();
    const trimmedPathName = pathname.endsWith('/') ? pathname.slice(0, pathname.length - 1) : pathname;
    // Match against the regex
    const match = trimmedPathName.match(ENTITY_TAB_NAME_REGEX_PATTERN);
    if (match && match[1]) {
        const selectedTabPath = match[1];
        const routedTab = tabs.find((tab) => tab.name === selectedTabPath);
        return routedTab;
    }
    // No match found!
    return undefined;
}

export function formatDateString(time: number) {
    const date = new Date(time);
    return date.toLocaleDateString('en-US');
}

export function useUpdateGlossaryEntityDataOnChange(
    entityData: GenericEntityProperties | null,
    entityType: EntityType,
) {
    const { setEntityData } = useGlossaryEntityData();
    const previousEntityData = usePrevious(entityData);

    useEffect(() => {
        // first check this is a glossary entity to prevent unnecessary comparisons in non-glossary context
        if (GLOSSARY_ENTITY_TYPES.includes(entityType) && !isEqual(entityData, previousEntityData)) {
            setEntityData(entityData);
        }
    });
}

export function getOnboardingStepIdsForEntityType(entityType: EntityType): string[] {
    switch (entityType) {
        case EntityType.Chart:
            return [
                ENTITY_PROFILE_V2_DOCUMENTATION_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_LINEAGE_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
        case EntityType.Container:
            return [
                ENTITY_PROFILE_V2_CONTENTS_ID,
                ENTITY_PROFILE_V2_DOCUMENTATION_ID,
                ENTITY_PROFILE_PROPERTIES_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_DOMAINS_ID,
            ];
        case EntityType.Dataset:
            return [
                ENTITY_PROFILE_V2_SIDEBAR_ID,
                ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
                ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
                ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
                ENTITY_SIDEBAR_V2_PROPERTIES_ID,
                ENTITY_PROFILE_DOMAINS_ID,
                ENTITY_PROFILE_OWNERS_ID,
                ENTITY_PROFILE_TAGS_ID,
                ENTITY_PROFILE_GLOSSARY_TERMS_ID,
                ENTITY_PROFILE_V2_COLUMNS_ID,
                ENTITY_PROFILE_V2_DOCUMENTATION_ID,
                ENTITY_PROFILE_LINEAGE_ID,
                ENTITY_PROFILE_V2_QUERIES_ID,
                ENTITY_PROFILE_V2_VALIDATION_ID,
                ENTITY_PROFILE_V2_INCIDENTS_ID,
            ];
        default:
            return [];
    }
}

export const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
};

const getFinalSidebarTabs = (
    tabs: EntitySidebarTab[],
    sidebarSections: EntitySidebarSection[],
    t: (key: string) => string,
) => {
    const sidebarTabsWithDefaults = tabs.map((tab) => ({
        ...tab,
        display: { ...defaultTabDisplayConfig, ...tab.display },
    }));

    let finalTabs = sidebarTabsWithDefaults;

    // Add a default "About" tab if only the legacy sections were provided.
    if ((sidebarSections || [])?.length > 0) {
        finalTabs = [
            {
                name: t('profile.defaultSummaryTabLabel'),
                icon: BookOpen,
                component: EntitySidebarSectionsTab,
                properties: {
                    sections: sidebarSections || [],
                },
                display: {
                    ...defaultTabDisplayConfig,
                },
            },
            ...sidebarTabsWithDefaults,
        ];
    }

    return finalTabs;
};

export function getPopularityColumn(tier: PopularityTier, t: (key: string) => string): SidebarStatsColumn | null {
    if (tier === undefined) return null;

    const status = getBarsStatusFromPopularityTier(tier);
    if (status) {
        return {
            title: t('profile.popularityColumnTitle'),
            content: <SidebarPopularityHeaderSection />,
        };
    }
    return null;
}

/**
 * Hook to get final sidebar tabs with all additions applied.
 * In OSS, this is a simple wrapper around getFinalSidebarTabs.
 * In SaaS, additional tabs like "Ask DataHub" are added on top.
 */
export function useFinalSidebarTabs(
    baseTabs: EntitySidebarTab[],
    sidebarSections: EntitySidebarSection[] | undefined,
    contextType: TabContextType,
) {
    const { t } = useTranslation('entity.shared.containers');
    const extraTabs = useExtraSidebarTabs(baseTabs, contextType);
    return useMemo(() => getFinalSidebarTabs(extraTabs, sidebarSections || [], t), [extraTabs, sidebarSections, t]);
}
