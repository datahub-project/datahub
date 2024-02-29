import { useState, useEffect } from 'react';

import { WatchQueryFetchPolicy } from '@apollo/client';

import { Form, FormType, FilterOperator } from '../../../../types.generated';
import { SearchCfg } from '../../../../conf';

import { useUserContext } from '../../../context/useUserContext';
import useFormFilter from './useFormFilter';
import useGetSearchQueryInputs from '../../../search/useGetSearchQueryInputs';


import {
	useGetFormQuery,
	useSearchForEntityUrnsByFormQuery,
	useSearchForEntitiesByFormCountQuery,
	useSearchForEntitiesByFormQuery,
	SearchForEntitiesByFormQuery
} from '../../../../graphql/form.generated';
import { generateFormCompletionFilter } from './utils';

// Common query configs
export const config = {
	searchFlags: { skipCache: true },
	fetchPolicy: 'no-cache' as WatchQueryFetchPolicy
}

export const useEntityFormDataFactory = (formUrn, selectedPromptId, formView, submittedEntities) => {
	const { user, refetchUnfinishedTaskCount } = useUserContext();
	const { query, orFilters, page } = useGetSearchQueryInputs();

	const [searchResults, setSearchResults] = useState();
	const [numResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);
	const [isLoading, setIsLoading] = useState(true);

	// Get current form data
	const {
		data: formData,
		loading: formDataLoading,
		refetch: refetchFormData
	} = useGetFormQuery({
		variables: { urn: formUrn },
		skip: !formUrn
	});

	// Form validator consts
	const form = formData?.form as Form;
	const isVerificationType = form?.info.type === FormType.Verification;

	// Form Filter
	const { formFilter, formResponsesFilters, setFormResponsesFilters } = useFormFilter({
		formUrn,
		formView,
		isVerificationType,
		selectedPromptId // can be undefined
	});

	// Search Data 
	const {
		data: searchData,
		loading: searchLoading,
		error: searchError,
		refetch: refetchSearch
	} = useSearchForEntitiesByFormQuery({
		variables: {
			input: {
				query,
				formFilter,
				start: (page - 1) * numResultsPerPage,
				count: numResultsPerPage,
				orFilters,
				searchFlags: config.searchFlags,
				// sortInput, TODO: support sorting on this endpoint
			},
		},
		fetchPolicy: config.fetchPolicy,
		skip: !formFilter.formUrn,
		notifyOnNetworkStatusChange: true,
	});

	// Get urns of assets assigned to form that need to be completed (for by entity navigation)
	const {
		data: entityUrnsByForm,
		loading: entityUrnsByFormLoading,
		refetch: refetchEntityUrnsByForm
	} = useSearchForEntityUrnsByFormQuery({
		variables: {
			input: {
				query: "*",
				formFilter: {
					formUrn,
					assignedActor: user?.urn,
					...generateFormCompletionFilter(formView, isVerificationType)
				},
				start: 0,
				count: 1000,
				searchFlags: config.searchFlags
			},
		},
		fetchPolicy: config.fetchPolicy,
		skip: !formUrn,
	});

	// Get count of assets with this form not complete - used for COMPLETION Form only
	const {
		data: assetsWithFormNotComplete,
		loading: assetsWithFormNotCompleteLoading,
		refetch: refetchAssetsWithFormNotComplete
	} = useSearchForEntitiesByFormCountQuery({
		variables: {
			input: {
				query: "*",
				formFilter: {
					formUrn,
					assignedActor: user?.urn,
					isFormComplete: false,
				},
				searchFlags: config.searchFlags
			},
		},
		fetchPolicy: config.fetchPolicy,
		skip: isVerificationType || !user?.urn || !formUrn,
	});

	// Get count of entities assigned to form that are ready to be verified
	const {
		data: assetsReadyForVerification,
		loading: assetsReadyForVerificationLoading,
		refetch: refetchAssetsReadyForVerification
	} = useSearchForEntitiesByFormCountQuery({
		variables: {
			input: {
				query: "*",
				formFilter: {
					formUrn,
					assignedActor: user?.urn,
					isFormVerified: false,
					isFormComplete: true,
				},
				searchFlags: config.searchFlags
			},
		},
		fetchPolicy: config.fetchPolicy,
		skip: !isVerificationType || !user?.urn || !formUrn,
	});

	// Get count of entities assigned to form that are verified
	const {
		data: assetsNotReadyForVerification,
		loading: assetsNotReadyForVerificationLoading,
		refetch: refetchAssetsNotReadyForVerification
	} = useSearchForEntitiesByFormCountQuery({
		variables: {
			input: {
				query: "*",
				formFilter: {
					formUrn,
					assignedActor: user?.urn,
					isFormVerified: false,
					isFormComplete: false,
				},
				searchFlags: config.searchFlags
			},
		},
		fetchPolicy: config.fetchPolicy,
		skip: !isVerificationType || !user?.urn || !formUrn,
	});

	// Get count of assets with the given prompt complete
	const {
		data: assetsWithPromptComplete,
		loading: assetsWithPromptCompleteLoading,
		refetch: refetchAssetsWithPromptComplete
	} = useSearchForEntitiesByFormCountQuery({
		variables: {
			input: {
				query: "*",
				formFilter: {
					formUrn,
					assignedActor: user?.urn,
					promptId: selectedPromptId,
					isPromptComplete: true,
					...generateFormCompletionFilter(formView, isVerificationType)
				},
				searchFlags: config.searchFlags
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
				query: "*",
				formFilter: {
					formUrn,
					assignedActor: user?.urn,
					promptId: selectedPromptId,
					isPromptComplete: false,
					...generateFormCompletionFilter(formView, isVerificationType)
				},
				searchFlags: config.searchFlags
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

	const optimisticSearchRefetch = () => {
		const status = {
			pending: true,
			success: undefined as boolean | undefined,
		}

		const prevResults = searchResults;

		refetchSearch({
			input: {
				query,
				formFilter,
				start: (page - 1) * numResultsPerPage,
				count: numResultsPerPage,
				orFilters: [...orFilters, {
					and: [
						{
							field: 'urn',
							condition: FilterOperator.Equal,
							values: submittedEntities,
							negated: true
						}
					]
				}],
				searchFlags: config.searchFlags,
			},
		})
			.catch(() => {
				// If fail, revert results
				setSearchResults(prevResults);
				status.pending = false;
				status.success = false;
			})
			.then((res) => {
				if (res) {
					// If success, set new results
					setSearchResults(res as any);
					status.pending = false;
					status.success = true;
				}
			});

		return status;
	};

	// Grouped Loading
	const loading = formDataLoading
		|| entityUrnsByFormLoading
		|| assetsReadyForVerificationLoading
		|| assetsNotReadyForVerificationLoading
		|| assetsWithPromptCompleteLoading
		|| assetsWithPromptNotCompleteLoading
		|| assetsWithFormNotCompleteLoading
		|| searchLoading;

	// Grouped Refetch
	const refetch = (optimistic = false) => {
		if (!isLoading) {
			setIsLoading(true);

			// Conditional optimistic
			if (optimistic) {
				optimisticSearchRefetch();
			}

			// delay for elastic data lag
			setTimeout(() => {
				// Always not optimistic
				refetchFormData();
				refetchUnfinishedTaskCount();
				refetchEntityUrnsByForm();
				if (isVerificationType) {
					refetchAssetsReadyForVerification();
					refetchAssetsNotReadyForVerification();
				} else {
					refetchAssetsWithFormNotComplete();
				}
				refetchAssetsWithPromptComplete();
				refetchAssestsWithPromptNotComplete();

				// Conditional optimistic
				if (!optimistic) {
					refetchSearch();
				}

				if (!loading) setIsLoading(false);
			}, 3000);
		}
	}

	// Utils for form asset counts 
	const assignedToFormVerifyReady = assetsReadyForVerification?.searchAcrossEntities?.total || 0;
	const assignedToFormNotVerifyReady = assetsNotReadyForVerification?.searchAcrossEntities?.total || 0;
	const assignedToFormNotNotComplete = assetsWithFormNotComplete?.searchAcrossEntities?.total || 0;

	// Utils for prompt asset counts 
	const numCompleteForPrompt = assetsWithPromptComplete?.searchAcrossEntities?.total || 0;
	const numNotCompleteForPrompt = assetsWithPromptNotComplete?.searchAcrossEntities?.total || 0;

	// Init render, set loading to false after all items performed.
	if (isLoading) setIsLoading(false);

	return ({
		refetch,
		loading,
		filter: {
			formFilter,
			formResponsesFilters,
			setFormResponsesFilters,
		},
		data: {
			form,
			entityUrnsByForm,
			searchResults: searchResults ? searchResults as SearchForEntitiesByFormQuery : {},
			searchError,
			searchLoading,
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
				numComplete: numCompleteForPrompt
			},
		},
	})
}
