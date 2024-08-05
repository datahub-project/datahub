import React, { useEffect, useState } from 'react';

import styled from 'styled-components';

import { SearchResults as NotAlchemySearchResults } from '../../../search/SearchResults';
import { SearchFilters as NotAlchemySearchFilers } from '../../../search/filters/SearchFilters';

import { SearchResults } from '../../../searchV2/SearchResults';
import SearchFilters from '../../../searchV2/filters/SearchFilters';

import useGetSearchQueryInputs from '../../../search/useGetSearchQueryInputs';
import { useIsSearchV2 } from '../../../search/useSearchAndBrowseVersion';
import useSearchPage from '../../../search/useSearchPage';
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
import usePrevious from '../../../shared/usePrevious';
import BulkVerifyPromptModal from './BulkVerifyPromptModal';

import { useIsThemeV2 } from '../../../useIsThemeV2';
import ActiveTasks from './ActiveTasks';

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
        search: {
            results,
            resultItems,
            resultItemCount,
            error,
            loading,
            refetch,
            numResultsPerPage,
            setNumResultsPerPage,
        },
        filter: { setFormResponsesFilters },
        entity: {
            selectedEntities,
            setSelectedEntities,
            setNumSubmittedEntities,
            areAllEntitiesSelected,
            setAreAllEntitiesSelected,
        },
        states: {
            byQuestion: { showVerifyCTA },
        },
    } = useEntityFormContext();

    const isV2 = useIsThemeV2();

    const showSearchFiltersV2 = useIsSearchV2();
    const { query, unionType, filters, viewUrn, page } = useGetSearchQueryInputs();
    const { filterMode, setFilterMode } = useFilterMode(filters, unionType);
    const [isVerifyModalVisible, setIsVerifyModalVisible] = useState(false);

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
        setAreAllEntitiesSelected(false);
        onChangeFilters([]);
    };

    const handleViewRemaining = () => {
        setFormResponsesFilters([FormResponsesFilter.INCOMPLETE]);
        onChangeFilters([]);
        setSelectedEntities([]);
        setNumSubmittedEntities(0);
    };

    const previousShowVerifyCTA = usePrevious(showVerifyCTA);
    useEffect(() => {
        if (showVerifyCTA && previousShowVerifyCTA === false) {
            setIsVerifyModalVisible(true);
        }
    }, [showVerifyCTA, previousShowVerifyCTA]);

    // Returns the non-Alchemy supported version
    if (!isV2) {
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
                        <NotAlchemySearchFilers
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
                <NotAlchemySearchResults
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
                    customSection={<EmptyStates closeModal={closeModal} handleViewRemaining={handleViewRemaining} />}
                    showCustomSection={resultItemCount === 0}
                    onClickExploreAll={clearAllFilters}
                    onClickClearFilters={clearAllFilters}
                    areAllEntitiesSelected={areAllEntitiesSelected}
                    setAreAllEntitiesSelected={setAreAllEntitiesSelected}
                    shouldHideSuggestions
                />
                <BulkVerifyPromptModal
                    isVisible={isVerifyModalVisible}
                    closeModal={() => setIsVerifyModalVisible(false)}
                />
                <ActiveTasks />
            </FormByQuestionWrapper>
        );
    }

    // Returns the Alchemy supported version
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
                        onChangeFilters={onChangeFilters}
                        onClearFilters={clearAllFilters}
                        onChangeUnionType={onChangeUnionType}
                        basicFilters
                    />
                </div>
            )}
            {resultItemCount === 0 && <EmptyStates closeModal={closeModal} handleViewRemaining={handleViewRemaining} />}
            {resultItemCount > 0 && (
                <SearchResults
                    unionType={unionType}
                    downloadSearchResults={downloadSearchResults}
                    page={page}
                    query={query}
                    viewUrn={viewUrn || undefined}
                    error={error}
                    searchResponse={loading ? undefined : results?.searchAcrossEntities}
                    availableFilters={loading ? [] : results?.searchAcrossEntities?.facets || []}
                    suggestions={results?.searchAcrossEntities?.suggestions || []}
                    selectedFilters={filters}
                    loading={loading}
                    onChangeFilters={onChangeFilters}
                    onChangePage={onChangePage}
                    numResultsPerPage={numResultsPerPage}
                    setNumResultsPerPage={setNumResultsPerPage}
                    setIsSelectMode={setIsSelectMode}
                    selectedEntities={selectedEntities}
                    setSelectedEntities={setSelectedEntities}
                    onChangeSelectAll={onChangeSelectAll}
                    refetch={refetch}
                    areAllEntitiesSelected={areAllEntitiesSelected}
                    setAreAllEntitiesSelected={setAreAllEntitiesSelected}
                    isSelectMode
                />
            )}
            <BulkVerifyPromptModal isVisible={isVerifyModalVisible} closeModal={() => setIsVerifyModalVisible(false)} />
            <ActiveTasks />
        </FormByQuestionWrapper>
    );
}
