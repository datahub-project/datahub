import { ButtonTabs, Tab } from '@components';
import React, { useCallback, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SelectFilterValuesTab } from '@app/entityV2/view/builder/SelectFilterValuesTab';
import {
    BUILD_FILTERS_TAB_KEY,
    DEFAULT_DYNAMIC_FILTER,
    SELECT_ASSETS_TAB_KEY,
    URN_FILTER_NAME,
} from '@app/entityV2/view/builder/constants';
import { ViewBuilderMode, ViewFilter } from '@app/entityV2/view/builder/types';
import { useViewBuilderProperties } from '@app/entityV2/view/builder/useViewBuilderProperties';
import {
    buildViewDefinition,
    filtersToLogicalPredicate,
    filtersToSelectedUrns,
    getInitialTabKey,
    logicalPredicateToFilters,
    selectedUrnsToFilters,
} from '@app/entityV2/view/builder/utils';
import { ViewBuilderState } from '@app/entityV2/view/types';
import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { EntityType, LogicalOperator } from '@types';

const ScrollableFiltersWrapper = styled.div`
    max-height: 300px;
    overflow-y: auto;
`;

const ReadOnlyWrapper = styled.div`
    pointer-events: none;
    opacity: 0.75;
`;

type Props = {
    mode: ViewBuilderMode;
    state: ViewBuilderState;
    updateState: (newState: ViewBuilderState) => void;
};

export const ViewDefinitionBuilder = ({ mode, state, updateState }: Props) => {
    const { t } = useTranslation('entity.views');
    const properties = useViewBuilderProperties();
    const existingFilters = (state.definition?.filter?.filters || []) as ViewFilter[];
    const existingOperator = state.definition?.filter?.operator;
    const existingEntityTypes = (state.definition?.entityTypes || []) as EntityType[];

    const [activeTab, setActiveTab] = useState(() => getInitialTabKey(existingFilters));

    // State for Select Assets tab
    const [selectedUrns, setSelectedUrns] = useState<string[]>(() => filtersToSelectedUrns(existingFilters));

    // State for Build Filters tab. Seed from both the saved filters and the view's
    // top-level entityTypes so the entity-type scope is visible/editable. Seeded
    // regardless of the active tab: a view can open on Select Assets and still
    // carry scope, and switching tabs serializes this state — leaving it null
    // there would clear the saved definition on a mere tab click. URN filters are
    // excluded because they belong to the Select Assets tab, not this one.
    const [dynamicFilter, setDynamicFilter] = useState<LogicalPredicate | null>(() => {
        const seedFilters = existingFilters.filter((filter) => filter.field !== URN_FILTER_NAME);
        if (seedFilters.length > 0 || existingEntityTypes.length > 0) {
            return filtersToLogicalPredicate(existingOperator, seedFilters, existingEntityTypes);
        }
        return null;
    });

    // Use a ref to access current state without adding it to effect dependencies
    const stateRef = useRef(state);
    stateRef.current = state;

    // Update parent state when Select Assets tab changes.
    // URN selections always use OR so the view matches any of the selected assets.
    const handleSelectAssetsChange = useCallback(
        (newUrns: string[]) => {
            setSelectedUrns(newUrns);
            const filters = selectedUrnsToFilters(newUrns);
            updateState({ ...stateRef.current, definition: buildViewDefinition(LogicalOperator.Or, filters) });
        },
        [updateState],
    );

    // Update parent state when Build Filters tab changes
    const handleDynamicFilterChange = useCallback(
        (newPredicate?: LogicalPredicate) => {
            setDynamicFilter(newPredicate || null);
            if (newPredicate) {
                const { operator, filters } = logicalPredicateToFilters(newPredicate);
                updateState({ ...stateRef.current, definition: buildViewDefinition(operator, filters) });
            } else {
                updateState({ ...stateRef.current, definition: buildViewDefinition(LogicalOperator.And, []) });
            }
        },
        [updateState],
    );

    const handleTabChange = useCallback(
        (newTabKey: string) => {
            setActiveTab(newTabKey);
            if (newTabKey === SELECT_ASSETS_TAB_KEY) {
                const filters = selectedUrnsToFilters(selectedUrns);
                updateState({ ...stateRef.current, definition: buildViewDefinition(LogicalOperator.Or, filters) });
            } else {
                const { operator, filters } = logicalPredicateToFilters(dynamicFilter);
                updateState({ ...stateRef.current, definition: buildViewDefinition(operator, filters) });
            }
        },
        [selectedUrns, dynamicFilter, updateState],
    );

    const isDisabled = mode === ViewBuilderMode.PREVIEW;

    const tabs: Tab[] = [
        {
            key: BUILD_FILTERS_TAB_KEY,
            label: t('viewDefinition.tabBuildFilters'),
            content: (
                <ScrollableFiltersWrapper>
                    <LogicalFiltersBuilder
                        filters={dynamicFilter ?? DEFAULT_DYNAMIC_FILTER}
                        onChangeFilters={handleDynamicFilterChange}
                        properties={properties}
                        hideAddGroup
                    />
                </ScrollableFiltersWrapper>
            ),
        },
        {
            key: SELECT_ASSETS_TAB_KEY,
            label: t('viewDefinition.tabSelectAssets'),
            content: (
                <SelectFilterValuesTab selectedUrns={selectedUrns} onChangeSelectedUrns={handleSelectAssetsChange} />
            ),
        },
    ];

    if (isDisabled) {
        return (
            <ReadOnlyWrapper>
                <ButtonTabs tabs={tabs} defaultKey={activeTab} onTabClick={handleTabChange} />
            </ReadOnlyWrapper>
        );
    }

    return <ButtonTabs tabs={tabs} defaultKey={activeTab} onTabClick={handleTabChange} />;
};
