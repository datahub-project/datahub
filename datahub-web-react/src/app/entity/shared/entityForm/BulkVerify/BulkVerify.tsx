import React, { useState } from 'react';

import styled from 'styled-components';
import { useParams } from 'react-router-dom';

import { SearchResults as NotAlchemySearchResults } from '../../../../search/SearchResults';
import { SearchFilters as NotAlchemySearchFilers } from '../../../../search/filters/SearchFilters';

import { SearchResults } from '../../../../searchV2/SearchResults';
import SearchFilters from '../../../../searchV2/filters/SearchFilters';

import { SearchCfg } from '../../../../../conf';
import { Entity } from '../../../../../types.generated';
import { PreviewType } from '../../../Entity';

import useGetSearchQueryInputs from '../../../../search/useGetSearchQueryInputs';
import useSearchPage from '../../../../search/useSearchPage';
import useFilterMode from '../../../../search/filters/useFilterMode';
import { FormResponsesFilter, useEntityFormContext } from '../EntityFormContext';
import { useIsSearchV2 } from '../../../../search/useSearchAndBrowseVersion';
import { OnboardingTour } from '../../../../onboarding/OnboardingTour';
import {
    FORM_BULK_VERIFY_ID,
    FORM_BULK_VERIFY_INTRO_ID,
    FORM_CHECK_RESPONSES_ID,
} from '../../../../onboarding/config/FormOnboardingConfig';

import { extractTypeFromUrn } from '../../utils';

import { BulkVerifyEntityModal } from './BulkVerifyEntityModal';

import { EmptyStates } from '../EmptyStates';

import { useIsThemeV2 } from '../../../../useIsThemeV2';
import ActiveTasks from '../ActiveTasks';

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
