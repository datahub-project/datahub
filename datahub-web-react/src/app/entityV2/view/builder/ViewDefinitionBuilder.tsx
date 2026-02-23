import React, { useCallback, useRef, useState } from 'react';
import styled from 'styled-components';

import { SelectFilterValuesTab } from '@app/entityV2/view/builder/SelectFilterValuesTab';
import {
    BUILD_FILTERS_TAB_KEY,
    DEFAULT_DYNAMIC_FILTER,
    SELECT_ASSETS_TAB_KEY,
} from '@app/entityV2/view/builder/constants';
import { ViewBuilderMode, ViewFilter } from '@app/entityV2/view/builder/types';
import {
    buildViewDefinition,
    filtersToLogicalPredicate,
    filtersToSelectedUrns,
    getInitialTabKey,
    logicalPredicateToFilters,
    selectedUrnsToFilters,
} from '@app/entityV2/view/builder/utils';
import { viewBuilderProperties } from '@app/entityV2/view/builder/viewBuilderProperties';
import { ViewBuilderState } from '@app/entityV2/view/types';
import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';
import { Tab } from '@app/homeV3/modules/shared/ButtonTabs/types';
import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { LogicalOperator } from '@types';

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
    const existingFilters = (state.definition?.filter?.filters || []) as ViewFilter[];
    const existingOperator = state.definition?.filter?.operator;

    const [activeTab, setActiveTab] = useState(() => getInitialTabKey(existingFilters));

    // State for Select Assets tab
    const [selectedUrns, setSelectedUrns] = useState<string[]>(() => filtersToSelectedUrns(existingFilters));

    // State for Build Filters tab
    const [dynamicFilter, setDynamicFilter] = useState<LogicalPredicate | null>(() => {
        if (existingFilters.length > 0 && activeTab === BUILD_FILTERS_TAB_KEY) {
            return filtersToLogicalPredicate(existingOperator, existingFilters);
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
            label: 'Build Filters',
            content: (
                <ScrollableFiltersWrapper>
                    <LogicalFiltersBuilder
                        filters={dynamicFilter ?? DEFAULT_DYNAMIC_FILTER}
                        onChangeFilters={handleDynamicFilterChange}
                        properties={viewBuilderProperties}
                        hideAddGroup
                    />
                </ScrollableFiltersWrapper>
            ),
        },
        {
            key: SELECT_ASSETS_TAB_KEY,
            label: 'Select Assets',
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
