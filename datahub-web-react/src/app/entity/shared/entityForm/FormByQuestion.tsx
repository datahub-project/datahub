import React, { useState } from 'react';

import styled from 'styled-components';

import { SearchResults } from '../../../search/SearchResults';
import useGetSearchQueryInputs from '../../../search/useGetSearchQueryInputs';
import { useIsSearchV2 } from '../../../search/useSearchAndBrowseVersion';
import useSearchPage from '../../../search/useSearchPage';
import { SearchCfg } from '../../../../conf';
import SearchFilters from '../../../search/filters/SearchFilters';
import useFilterMode from '../../../search/filters/useFilterMode';
import { FormResponsesFilter, useEntityFormContext } from './EntityFormContext';
import { OnboardingTour } from '../../../onboarding/OnboardingTour';
import {
    FORM_ANSWER_IN_BULK_ID,
    FORM_ASSETS_ASSIGNED_ID,
    FORM_FILTER_AND_BROWSE_ID,
    WELCOME_TO_BULK_BY_QUESTION_ID,
} from '../../../onboarding/config/FormOnboardingConfig';

import { EmptyStates } from './EmptyStates';

const FormByQuestionWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

interface Props {
    closeModal: () => void;
}

export default function FormByQuestion({ closeModal }: Props) {
    const {
        refetch,
        search: {
            results,
            resultItems,
            resultItemCount,
            error,
            loading,
        },
        filter: {
            setFormResponsesFilters,
        },
        entity: {
            selectedEntities,
            setSelectedEntities,
            setNumSubmittedEntities,
        },
    } = useEntityFormContext();

    const showSearchFiltersV2 = useIsSearchV2();
    const { query, unionType, filters, viewUrn, page } = useGetSearchQueryInputs();
    const { filterMode, setFilterMode } = useFilterMode(filters, unionType);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const {
        isSelectMode,
        setIsSelectMode,
        downloadSearchResults,
        onChangeFilters,
        onChangeUnionType,
        onChangePage,
        onChangeSelectAll,
    } = useSearchPage({
        searchResults: resultItems,
        currentPath: window.location.pathname,
        selectedEntities,
        setSelectedEntities,
        defaultIsSelectMode: true,
    });

    const clearAllFilters = () => {
        setFormResponsesFilters([]);
        setSelectedEntities([]);
        onChangeFilters([]);
    };

    const handleViewRemaining = () => {
        setFormResponsesFilters([FormResponsesFilter.INCOMPLETE]);
        onChangeFilters([]);
        setSelectedEntities([]);
        setNumSubmittedEntities(0);
    };

    return (
        <FormByQuestionWrapper>
            <OnboardingTour
                stepIds={[
                    WELCOME_TO_BULK_BY_QUESTION_ID,
                    FORM_ASSETS_ASSIGNED_ID,
                    FORM_FILTER_AND_BROWSE_ID,
                    FORM_ANSWER_IN_BULK_ID,
                ]}
            />
            {showSearchFiltersV2 && (
                <div id={FORM_FILTER_AND_BROWSE_ID}>
                    <SearchFilters
                        loading={loading}
                        availableFilters={loading ? [] : results?.searchAcrossEntities?.facets || []}
                        activeFilters={filters}
                        unionType={unionType}
                        mode={filterMode}
                        onChangeFilters={onChangeFilters}
                        onClearFilters={clearAllFilters}
                        onChangeUnionType={onChangeUnionType}
                        onChangeMode={setFilterMode}
                    />
                </div>
            )}
            <SearchResults
                unionType={unionType}
                downloadSearchResults={downloadSearchResults}
                page={page}
                query={query}
                viewUrn={viewUrn || undefined}
                error={error}
                searchResponse={loading ? undefined : results?.searchAcrossEntities}
                facets={results?.searchAcrossEntities?.facets}
                suggestions={results?.searchAcrossEntities?.suggestions || []}
                selectedFilters={filters}
                loading={loading}
                onChangeFilters={onChangeFilters}
                onChangeUnionType={onChangeUnionType}
                onChangePage={onChangePage}
                numResultsPerPage={numResultsPerPage}
                setNumResultsPerPage={setNumResultsPerPage}
                isSelectMode={isSelectMode}
                selectedEntities={selectedEntities}
                setSelectedEntities={setSelectedEntities}
                setIsSelectMode={setIsSelectMode}
                onChangeSelectAll={onChangeSelectAll}
                refetch={refetch}
                customSection={
                    <EmptyStates
                        closeModal={closeModal}
                        handleViewRemaining={handleViewRemaining}
                    />
                }
                showCustomSection={resultItemCount === 0}
                onClickExploreAll={clearAllFilters}
                onClickClearFilters={clearAllFilters}
                shouldHideSuggestions
            />
        </FormByQuestionWrapper>
    );
}
