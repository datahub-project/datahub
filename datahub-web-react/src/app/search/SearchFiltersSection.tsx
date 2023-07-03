import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { FacetFilterInput, FacetMetadata } from '../../types.generated';
import { UnionType } from './utils/constants';
import { hasAdvancedFilters } from './utils/hasAdvancedFilters';
import { AdvancedSearchFilters } from './AdvancedSearchFilters';
import { SimpleSearchFilters } from './SimpleSearchFilters';
import { SEARCH_RESULTS_ADVANCED_SEARCH_ID } from '../onboarding/config/SearchOnboardingConfig';
import { ViewBuilder } from '../entity/view/builder/ViewBuilder';
import { buildInitialViewState, fromUnionType } from '../entity/view/builder/utils';
import { SaveAsViewButton } from './SaveAsViewButton';
import { useUserContext } from '../context/useUserContext';
import { ViewBuilderMode } from '../entity/view/builder/types';

type Props = {
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    unionType: UnionType;
    loading: boolean;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangeUnionType: (unionType: UnionType) => void;
};

const FiltersContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 260px;
    min-width: 260px;
    overflow-wrap: break-word;
    border-right: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    height: 100%;
`;

const FiltersHeader = styled.div`
    font-size: 14px;
    font-weight: 600;

    padding-left: 20px;
    padding-right: 4px;
    padding-bottom: 8px;

    width: 100%;
    height: 47px;
    line-height: 47px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};

    justify-content: space-between;
    display: flex;
`;

const SearchFiltersWrapper = styled.div`
    max-height: 100%;
    padding-top: 10px;
    overflow: auto;

    &::-webkit-scrollbar {
        height: 12px;
        width: 1px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

const AdvancedSearchFiltersWrapper = styled.div`
    margin-top: 6px;
    margin-left: 12px;
    margin-right: 12px;
`;

// This component renders the entire filters section that allows toggling
// between the simplified search experience and advanced search
export const SearchFiltersSection = ({
    filters,
    selectedFilters,
    unionType,
    loading,
    onChangeFilters,
    onChangeUnionType,
}: Props) => {
    const userContext = useUserContext();
    const onlyShowAdvancedFilters = hasAdvancedFilters(selectedFilters, unionType);
    const [showViewBuilder, setShowViewBuilder] = useState(false);
    const [seeAdvancedFilters, setSeeAdvancedFilters] = useState(onlyShowAdvancedFilters);

    const onSaveAsView = () => {
        setShowViewBuilder(true);
    };

    // Only show "Save As View" if there are selected Filters, and there is no
    // current View applied (creating a new Filter on top of an existing View is not currently supported).
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;
    const showSaveAsView = selectedFilters?.length > 0 && selectedViewUrn === undefined;

    return (
        <FiltersContainer>
            <FiltersHeader>
                <span>Filter</span>
                <span>
                    <Button
                        disabled={onlyShowAdvancedFilters}
                        type="link"
                        onClick={() => setSeeAdvancedFilters(!seeAdvancedFilters)}
                        id={SEARCH_RESULTS_ADVANCED_SEARCH_ID}
                    >
                        {seeAdvancedFilters ? 'Basic' : 'Advanced'}
                    </Button>
                </span>
            </FiltersHeader>
            <SearchFiltersWrapper>
                {seeAdvancedFilters ? (
                    <AdvancedSearchFiltersWrapper>
                        <AdvancedSearchFilters
                            unionType={unionType}
                            selectedFilters={selectedFilters}
                            onFilterSelect={(newFilters) => onChangeFilters(newFilters)}
                            onChangeUnionType={onChangeUnionType}
                            facets={filters || []}
                            loading={loading}
                        />
                        {showSaveAsView && <SaveAsViewButton onClick={onSaveAsView} />}
                        {showViewBuilder && (
                            <ViewBuilder
                                mode={ViewBuilderMode.EDITOR}
                                initialState={buildInitialViewState(selectedFilters, fromUnionType(unionType))}
                                onSubmit={() => setShowViewBuilder(false)}
                                onCancel={() => setShowViewBuilder(false)}
                            />
                        )}
                    </AdvancedSearchFiltersWrapper>
                ) : (
                    <SimpleSearchFilters
                        loading={loading}
                        facets={filters || []}
                        selectedFilters={selectedFilters}
                        onFilterSelect={(newFilters) => onChangeFilters(newFilters)}
                    />
                )}
            </SearchFiltersWrapper>
        </FiltersContainer>
    );
};
