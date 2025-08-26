import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import ActiveTasks from '@app/entity/shared/entityForm/ActiveTasks';
import BulkVerifyPromptModal from '@app/entity/shared/entityForm/BulkVerifyPromptModal';
import { EmptyStates } from '@app/entity/shared/entityForm/EmptyStates';
import { FormResponsesFilter, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    FORM_ANSWER_IN_BULK_ID,
    FORM_ASSETS_ASSIGNED_ID,
    FORM_FILTER_AND_BROWSE_ID,
    WELCOME_TO_BULK_BY_QUESTION_ID,
} from '@app/onboarding/config/FormOnboardingConfig';
import { SearchResults as NotAlchemySearchResults } from '@app/search/SearchResults';
import { SearchFilters as NotAlchemySearchFilers } from '@app/search/filters/SearchFilters';
import useFilterMode from '@app/search/filters/useFilterMode';
import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';
import { useIsSearchV2 } from '@app/search/useSearchAndBrowseVersion';
import useSearchPage from '@app/search/useSearchPage';
import { SearchResults } from '@app/searchV2/SearchResults';
import SearchFilters from '@app/searchV2/filters/SearchFilters';
import usePrevious from '@app/shared/usePrevious';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { EmbeddedSearchBar } from '@src/app/searchV2/EmbeddedSearchBar';

const FormByQuestionWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const V1SearchBarWrapper = styled.div<{ $padding?: string }>`
    padding: ${(props) => (props.$padding ? props.$padding : '8px 20px')};
    border-bottom: 1px solid rgba(0, 0, 0, 0.06);
`;

const V2SearchBarWrapper = styled.div`
    padding: 8px 32px;
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

    const url = new URL(window.location.href);
    const formUrn = url.searchParams.get('form_urn');

    const {
        isSelectMode,
        setIsSelectMode,
        downloadSearchResults,
        onChangeFilters,
        onChangeUnionType,
        onChangePage,
        onChangeSelectAll,
        onChangeQuery,
    } = useSearchPage({
        searchResults: resultItems,
        currentPath: window.location.pathname,
        selectedEntities,
        setSelectedEntities,
        defaultIsSelectMode: true,
        existingSearchParams: formUrn ? { form_urn: formUrn } : undefined,
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
        onChangeQuery('');
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
                <V1SearchBarWrapper $padding={showSearchFiltersV2 ? '8px 24px' : undefined}>
                    <EmbeddedSearchBar />
                </V1SearchBarWrapper>
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
            <V2SearchBarWrapper>
                <EmbeddedSearchBar />
            </V2SearchBarWrapper>
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
                        query={query}
                        viewUrn={viewUrn ?? undefined}
                        totalResults={results?.searchAcrossEntities?.total || 0}
                        setShowSelectMode={setIsSelectMode}
                        downloadSearchResults={downloadSearchResults}
                    />
                </div>
            )}
            {resultItemCount === 0 && <EmptyStates closeModal={closeModal} handleViewRemaining={handleViewRemaining} />}
            {resultItemCount > 0 && (
                <SearchResults
                    page={page}
                    query={query}
                    error={error}
                    searchResponse={loading ? undefined : results?.searchAcrossEntities}
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
