import { WatchQueryFetchPolicy } from '@apollo/client';
import { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { FormResponsesFilter, FormView } from '@app/entity/shared/entityForm/EntityFormContext';
import useFormFilter from '@app/entity/shared/entityForm/useFormFilter';
import { generateFormCompletionFilter } from '@app/entity/shared/entityForm/utils';
import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';
import { SearchCfg } from '@src/conf';

import {
    SearchForEntitiesByFormQuery,
    useGetFormQuery,
    useSearchForEntitiesByFormCountQuery,
    useSearchForEntitiesByFormQuery,
    useSearchForEntityUrnsByFormQuery,
} from '@graphql/form.generated';
import { FilterOperator, Form, FormType } from '@types';

export const MAX_ENTITY_URN_COUNT = 1000;

// Common query configs
export const config = {
    searchFlags: { skipCache: true },
    fetchPolicy: 'no-cache' as WatchQueryFetchPolicy,
};

export const useEntityFormDataFactory = (
    formUrn,
    selectedPromptId,
    formView,
    submittedEntitiesMap,
    verifiedEntities,
) => {
    const { user } = useUserContext();
    const { query, orFilters, page, sortInput } = useGetSearchQueryInputs();

    const [searchResults, setSearchResults] = useState();
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);
    const [isLoading, setIsLoading] = useState(true);

    // Get current form data
    const { data: formData, loading: formDataLoading } = useGetFormQuery({
        variables: { urn: formUrn },
        skip: !formUrn,
    });

    // Form validator consts
    const form = formData?.form as Form;
    const isVerificationType = form?.info?.type === FormType.Verification;

    // Form Filter
    const { formFilter, formResponsesFilters, setFormResponsesFilters } = useFormFilter({
        formUrn,
        formView,
        isVerificationType,
        selectedPromptId, // can be undefined
    });

    // only filter urns out for optimistic rendering on by question view if the user is filtering to see incomplete entities (default)
    const shouldBeOptimisticForPrompt =
        formResponsesFilters.length === 1 && formResponsesFilters[0] === FormResponsesFilter.INCOMPLETE;
    const submittedEntitiesForPrompt = submittedEntitiesMap[selectedPromptId] || [];
    const entitiesToFilterForPrompt = shouldBeOptimisticForPrompt ? submittedEntitiesForPrompt : [];
    const entitiesToFilterOut = formView === FormView.BY_QUESTION ? entitiesToFilterForPrompt : verifiedEntities;
    const urnsFilter = {
        field: 'urn',
        condition: FilterOperator.Equal,
        values: entitiesToFilterOut,
        negated: true,
    };
    const combinedFilters = orFilters.length
        ? orFilters.map((andFilterInput) => ({ and: [...(andFilterInput.and || []), urnsFilter] }))
        : [{ and: [urnsFilter] }];

    // Search Data
    const {
        data: searchData,
        loading: searchLoading,
        error: searchError,
        refetch: refetchSearch,
    } = useSearchForEntitiesByFormQuery({
        variables: {
            input: {
                query,
                formFilter,
                start: (page - 1) * numResultsPerPage,
                count: numResultsPerPage,
                orFilters: combinedFilters,
                searchFlags: config.searchFlags,
                sortInput,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: !formFilter.formUrn,
        notifyOnNetworkStatusChange: true,
    });

    // Get urns of assets assigned to form that need to be completed (for by entity navigation)
    const { data: entityUrnsByForm, loading: entityUrnsByFormLoading } = useSearchForEntityUrnsByFormQuery({
        variables: {
            input: {
                query: '*',
                formFilter: {
                    formUrn,
                    assignedActor: user?.urn,
                    ...generateFormCompletionFilter(formView, isVerificationType),
                },
                start: 0,
                count: MAX_ENTITY_URN_COUNT,
                searchFlags: config.searchFlags,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: !formUrn,
    });

    // Get count of assets with this form not complete - used for COMPLETION Form only
    const {
        data: assetsWithFormNotComplete,
        loading: assetsWithFormNotCompleteLoading,
        refetch: refetchAssetsWithFormNotComplete,
    } = useSearchForEntitiesByFormCountQuery({
        variables: {
            input: {
                query: '*',
                formFilter: {
                    formUrn,
                    assignedActor: user?.urn,
                    isFormComplete: false,
                },
                searchFlags: config.searchFlags,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: isVerificationType || !user?.urn || !formUrn,
    });

    // Get count of entities assigned to form that are ready to be verified
    const {
        data: assetsReadyForVerification,
        loading: assetsReadyForVerificationLoading,
        refetch: refetchAssetsReadyForVerification,
    } = useSearchForEntitiesByFormCountQuery({
        variables: {
            input: {
                query: '*',
                formFilter: {
                    formUrn,
                    assignedActor: user?.urn,
                    isFormVerified: false,
                    isFormComplete: true,
                },
                orFilters: formView === FormView.BULK_VERIFY ? [{ and: [urnsFilter] }] : undefined,
                searchFlags: config.searchFlags,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: !isVerificationType || !user?.urn || !formUrn,
        notifyOnNetworkStatusChange: true,
    });

    // Get count of entities assigned to form that are verified
    const {
        data: assetsNotReadyForVerification,
        loading: assetsNotReadyForVerificationLoading,
        refetch: refetchAssetsNotReadyForVerification,
    } = useSearchForEntitiesByFormCountQuery({
        variables: {
            input: {
                query: '*',
                formFilter: {
                    formUrn,
                    assignedActor: user?.urn,
                    isFormVerified: false,
                    isFormComplete: false,
                },
                orFilters: formView === FormView.BULK_VERIFY ? [{ and: [urnsFilter] }] : undefined,
                searchFlags: config.searchFlags,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: !isVerificationType || !user?.urn || !formUrn,
        notifyOnNetworkStatusChange: true,
    });

    // Get count of assets with the given prompt complete
    const {
        data: assetsWithPromptComplete,
        loading: assetsWithPromptCompleteLoading,
        refetch: refetchAssetsWithPromptComplete,
    } = useSearchForEntitiesByFormCountQuery({
        variables: {
            input: {
                query: '*',
                formFilter: {
                    formUrn,
                    assignedActor: user?.urn,
                    promptId: selectedPromptId,
                    isPromptComplete: true,
                    ...generateFormCompletionFilter(formView, isVerificationType),
                },
                orFilters: [{ and: [urnsFilter] }],
                searchFlags: config.searchFlags,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: !selectedPromptId || !user?.urn || !formUrn,
    });

    // Get count of assets with the given prompt NOT complete
    const {
        data: assetsWithPromptNotComplete,
        loading: assetsWithPromptNotCompleteLoading,
        refetch: refetchAssestsWithPromptNotComplete,
    } = useSearchForEntitiesByFormCountQuery({
        variables: {
            input: {
                query: '*',
                formFilter: {
                    formUrn,
                    assignedActor: user?.urn,
                    promptId: selectedPromptId,
                    isPromptComplete: false,
                    ...generateFormCompletionFilter(formView, isVerificationType),
                },
                orFilters: [{ and: [urnsFilter] }],
                searchFlags: config.searchFlags,
            },
        },
        fetchPolicy: config.fetchPolicy,
        skip: !selectedPromptId || !user?.urn || !formUrn,
    });

    /*
     * Optimistic refetching
     */

    // Set inital state
    useEffect(() => {
        if (!searchError && searchData) setSearchResults(searchData as any);
    }, [searchData, searchError, setSearchResults]);

    // Grouped Loading
    const loading =
        formDataLoading ||
        entityUrnsByFormLoading ||
        assetsReadyForVerificationLoading ||
        assetsNotReadyForVerificationLoading ||
        assetsWithPromptCompleteLoading ||
        assetsWithPromptNotCompleteLoading ||
        assetsWithFormNotCompleteLoading ||
        searchLoading;

    // Update search and all counts for states in Bulk By Question view
    const refetchForBulk = () => {
        refetchSearch();
        if (isVerificationType) {
            refetchAssetsReadyForVerification();
            refetchAssetsNotReadyForVerification();
        } else {
            refetchAssetsWithFormNotComplete();
        }
        refetchAssetsWithPromptComplete();
        refetchAssestsWithPromptNotComplete();
    };

    const refetchNonOptimisticData = () => {
        if (isVerificationType) {
            refetchAssetsReadyForVerification();
            refetchAssetsNotReadyForVerification();
        } else {
            refetchAssetsWithFormNotComplete();
        }
    };

    // Utils for form asset counts
    const assignedToFormVerifyReady = assetsReadyForVerification?.searchAcrossEntities?.total || 0;
    const assignedToFormNotVerifyReady = assetsNotReadyForVerification?.searchAcrossEntities?.total || 0;
    const assignedToFormNotNotComplete = assetsWithFormNotComplete?.searchAcrossEntities?.total || 0;

    // Utils for prompt asset counts
    const numCompleteForPrompt =
        (assetsWithPromptComplete?.searchAcrossEntities?.total || 0) +
        (shouldBeOptimisticForPrompt ? submittedEntitiesForPrompt.length : 0);
    const numNotCompleteForPrompt = assetsWithPromptNotComplete?.searchAcrossEntities?.total || 0;

    // Init render, set loading to false after all items performed.
    if (isLoading) setIsLoading(false);

    return {
        refetchForBulk,
        loading,
        filter: {
            formFilter,
            orFilters: combinedFilters,
            formResponsesFilters,
            setFormResponsesFilters,
        },
        data: {
            form,
            entityUrnsByForm,
            searchResults: searchResults ? (searchResults as SearchForEntitiesByFormQuery) : {},
            searchError,
            searchLoading,
            refetchSearch,
            refetchNonOptimisticData,
            verificationDataLoading: assetsReadyForVerificationLoading || assetsNotReadyForVerificationLoading,
            numResultsPerPage,
            setNumResultsPerPage,
        },
        counts: {
            verificationType: {
                verifyReady: assignedToFormVerifyReady,
                notVerifyReady: assignedToFormNotVerifyReady,
            },
            completionType: {
                notComplete: assignedToFormNotNotComplete,
            },
            promptCounts: {
                numNotComplete: numNotCompleteForPrompt,
                numComplete: numCompleteForPrompt,
            },
        },
    };
};
