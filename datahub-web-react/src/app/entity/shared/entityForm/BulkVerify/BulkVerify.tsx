import React, { useState } from 'react';
import { useParams } from 'react-router-dom';
import styled from 'styled-components';

import { PreviewType } from '@app/entity/Entity';
import ActiveTasks from '@app/entity/shared/entityForm/ActiveTasks';
import { BulkVerifyEntityModal } from '@app/entity/shared/entityForm/BulkVerify/BulkVerifyEntityModal';
import { EmptyStates } from '@app/entity/shared/entityForm/EmptyStates';
import { FormResponsesFilter, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { extractTypeFromUrn } from '@app/entity/shared/utils';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    FORM_BULK_VERIFY_ID,
    FORM_BULK_VERIFY_INTRO_ID,
    FORM_CHECK_RESPONSES_ID,
} from '@app/onboarding/config/FormOnboardingConfig';
import { SearchResults as NotAlchemySearchResults } from '@app/search/SearchResults';
import { SearchFilters as NotAlchemySearchFilers } from '@app/search/filters/SearchFilters';
import useFilterMode from '@app/search/filters/useFilterMode';
import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';
import { useIsSearchV2 } from '@app/search/useSearchAndBrowseVersion';
import useSearchPage from '@app/search/useSearchPage';
import { SearchResults } from '@app/searchV2/SearchResults';
import SearchFilters from '@app/searchV2/filters/SearchFilters';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { SearchCfg } from '@src/conf';

import { Entity } from '@types';

const FormByQuestionWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

interface Props {
    closeFormModal: () => void;
}

export default function BulkVerify({ closeFormModal }: Props) {
    const { urn: paramUrn }: any = useParams();

    const {
        search: { results, resultItems, resultItemCount, error, loading, refetch },
        entity: {
            selectedEntities,
            setSelectedEntity,
            setSelectedEntities,
            setNumSubmittedEntities,
            areAllEntitiesSelected,
            setAreAllEntitiesSelected,
        },
        filter: { setFormResponsesFilters },
    } = useEntityFormContext();

    const isV2 = useIsThemeV2();

    const showSearchFiltersV2 = useIsSearchV2();
    const [isModalOpen, setIsModalOpen] = useState(false);
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
        onChangeFilters([]);
    };

    const openModal = () => setIsModalOpen(true);
    const closeModal = () => {
        const extractedUrn = paramUrn;
        if (extractedUrn) {
            const extractedType = extractTypeFromUrn(extractedUrn);
            setSelectedEntity({
                urn: extractedUrn,
                type: extractedType,
            });
        }
        closeFormModal();
    };

    const closeViewResponseModal = () => setIsModalOpen(false);

    const handleViewRemaining = () => {
        setFormResponsesFilters([FormResponsesFilter.INCOMPLETE]);
        onChangeFilters([]);
        setSelectedEntities([]);
        setNumSubmittedEntities(0);
    };

    const handleCardClick = (entity: Entity) => {
        setSelectedEntity(entity);
        openModal();
    };

    // Returns the non-Alchemy supported version
    if (!isV2) {
        return (
            <>
                {!!resultItemCount && (
                    <OnboardingTour
                        stepIds={[FORM_BULK_VERIFY_INTRO_ID, FORM_CHECK_RESPONSES_ID, FORM_BULK_VERIFY_ID]}
                    />
                )}
                <FormByQuestionWrapper>
                    {showSearchFiltersV2 && (
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
                        previewType={PreviewType.BULK_VERIFY}
                        onCardClick={handleCardClick}
                        customSection={
                            <EmptyStates closeModal={closeModal} handleViewRemaining={handleViewRemaining} />
                        }
                        showCustomSection={resultItemCount === 0}
                        shouldHideSuggestions
                        onClickExploreAll={clearAllFilters}
                        onClickClearFilters={clearAllFilters}
                        areAllEntitiesSelected={areAllEntitiesSelected}
                        setAreAllEntitiesSelected={setAreAllEntitiesSelected}
                    />
                </FormByQuestionWrapper>
                <BulkVerifyEntityModal isOpen={isModalOpen} onClose={closeViewResponseModal} />
                <ActiveTasks />
            </>
        );
    }

    // Returns the Alchemy supported version
    return (
        <>
            {!!resultItemCount && (
                <OnboardingTour stepIds={[FORM_BULK_VERIFY_INTRO_ID, FORM_CHECK_RESPONSES_ID, FORM_BULK_VERIFY_ID]} />
            )}
            <FormByQuestionWrapper>
                {showSearchFiltersV2 && (
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
                )}
                {resultItemCount === 0 && (
                    <EmptyStates closeModal={closeModal} handleViewRemaining={handleViewRemaining} />
                )}
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
                        previewType={PreviewType.BULK_VERIFY}
                        onCardClick={handleCardClick}
                        areAllEntitiesSelected={areAllEntitiesSelected}
                        setAreAllEntitiesSelected={setAreAllEntitiesSelected}
                        isSelectMode
                    />
                )}
            </FormByQuestionWrapper>
            <BulkVerifyEntityModal isOpen={isModalOpen} onClose={closeViewResponseModal} />
            <ActiveTasks />
        </>
    );
}
