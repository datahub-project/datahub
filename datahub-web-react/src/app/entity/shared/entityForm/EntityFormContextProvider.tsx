import React, { useEffect, useState } from 'react';

import { useEntityContext } from '../EntityContext';
import { useEntityFormDataFactory } from './entityFormDataFactory';
import usePrevious from '../../../shared/usePrevious';

import { EntityFormContext, FormView } from './EntityFormContext';
import { Entity, FormType } from '../../../../types.generated';
import { EntityAndType, GenericEntityProperties } from '../types';

import { getBulkByQuestionPrompts } from '../containers/profile/sidebar/FormInfo/utils';
import { SCHEMA_FIELD_PROMPT_TYPES } from './constants';
import { useEntityFormTasks } from './useEntityFormTasks';
import useEntityDataForForm from './useEntityDataForForm';

interface Props {
    children: React.ReactNode;
    formUrn: string;
    defaultFormView?: FormView;
}

export default function EntityFormContextProvider({ children, formUrn, defaultFormView }: Props) {
    // Import external contexts
    const { entityData, refetch: refetchEntityProfile, loading: profileLoading } = useEntityContext();

    /*
     * State setup
     */

    const [formView, setFormView] = useState<FormView>(defaultFormView || FormView.BY_ENTITY);
    const [selectedEntity, setSelectedEntity] = useState<Entity | undefined>(entityData as Entity);
    const [selectedPromptId, setSelectedPromptId] = useState<string>();
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [areAllEntitiesSelected, setAreAllEntitiesSelected] = useState(false);
    const [submittedEntities, setSubmittedEntities] = useState<EntityAndType[]>([]); // used for optimistic render
    const [numSubmittedEntities, setNumSubmittedEntities] = useState<number>(0);
    const [shouldClearFilters, setShouldClearFilters] = useState<boolean>(false);
    const [shouldRefetch, setShouldRefetch] = useState<boolean>(false);
    const [nonOptimisticLoading, setNonOptimisticLoading] = useState<boolean>(false);
    const [submittedEntitiesMap, setSubmittedEntitiesMap] = useState<{ [promptId: string]: string[] }>({}); // map from promptId: list of submitted entity urns
    const [verifiedEntities, setVerifiedEntities] = useState<string[]>([]);

    /*
     * Data setup
     */

    const {
        refetchForBulk,
        loading,
        data: {
            form,
            entityUrnsByForm,
            searchResults,
            searchError,
            searchLoading,
            refetchNonOptimisticData,
            refetchSearch,
            verificationDataLoading,
            numResultsPerPage,
            setNumResultsPerPage,
        },
        filter,
        counts,
    } = useEntityFormDataFactory(formUrn, selectedPromptId, formView, submittedEntitiesMap, verifiedEntities);

    // Async task management for mega-bulk submit
    const { activeTasks, completeTasks, handleAsyncBatchSubmit } = useEntityFormTasks(formUrn);

    // Determine the previous form's urn
    const previousFormUrn = usePrevious(formUrn);

    // Determine form type
    const isVerificationType = form?.info.type === FormType.Verification;

    // Find intitial prompt
    const initialPromptId =
        form?.info?.prompts?.filter((prompt) => !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type))[0]?.id || null;

    // Place current entity first in entity array
    const entitiesForForm = entityUrnsByForm?.searchAcrossEntities?.searchResults
        .map((result) => result.entity)
        .filter((result) => result.urn !== entityData?.urn);
    if (entityData) {
        entitiesForForm?.unshift(entityData as Entity);
    }

    // Grab the asset's data
    const { selectedEntityData, isOnEntityProfilePage, entityRefetch, entityLoading } = useEntityDataForForm({
        selectedEntity,
    });

    /*
     * Loading conslidation
     */

    const isProfileLoading = profileLoading || loading;
    const isDatasetLoading = entityLoading || loading;
    const isLoading = isOnEntityProfilePage ? isProfileLoading : isDatasetLoading;

    /*
     * Refetch conslidation
     */

    const handleRefetch = (): any => {
        if (isOnEntityProfilePage) refetchEntityProfile();
        else entityRefetch?.();
    };

    /*
     * Handling submissions
     */

    const handlePromptSubmission = (promptId: string, entityUrns: string[]) => {
        setSubmittedEntitiesMap({
            ...submittedEntitiesMap,
            [promptId]: [...(submittedEntitiesMap[promptId] || []), ...entityUrns],
        });
        setNonOptimisticLoading(true);
        setTimeout(() => {
            setShouldRefetch(true);
            refetchNonOptimisticData();
            setNonOptimisticLoading(false);
        }, 5000);
    };

    const handleUndoPromptSubmission = (promptId: string, entityUrns: string[]) => {
        const submittedUrnsForPrompt = submittedEntitiesMap[promptId] || [];
        setSubmittedEntitiesMap({
            ...submittedEntitiesMap,
            [promptId]: [...submittedUrnsForPrompt.filter((urn) => entityUrns.includes(urn))],
        });
    };

    const handleBulkVerifySubmission = (entityUrns: string[]) => {
        setVerifiedEntities([...verifiedEntities, ...entityUrns]);
        setTimeout(() => {
            setShouldRefetch(true);
        }, 5000);
    };

    /*
     * Pragmatic updates to state
     */

    useEffect(() => {
        if (!selectedEntity || (!selectedEntity.urn && entitiesForForm)) {
            const entity = entitiesForForm && entitiesForForm[0];
            setSelectedEntity(entity);
        }
    }, [entitiesForForm, setSelectedEntity, selectedEntity]);

    useEffect(() => {
        if (!selectedPromptId && initialPromptId) setSelectedPromptId(initialPromptId);
    }, [selectedPromptId, initialPromptId]);

    useEffect(() => {
        if (formUrn && previousFormUrn !== formUrn && initialPromptId) {
            setFormView(FormView.BY_ENTITY);
            setSelectedPromptId(initialPromptId);
        }
    }, [formUrn, previousFormUrn, initialPromptId]);

    /*
     * Consolidate context output
     */

    // Submission
    const submission = {
        nonOptimisticLoading,
        handlePromptSubmission,
        handleUndoPromptSubmission,
        handleBulkVerifySubmission,
        verificationDataLoading,
        activeTasks,
        completeTasks,
        handleAsyncBatchSubmit,
    };

    // Search
    const search = {
        results: searchResults,
        resultItems: searchResults?.searchAcrossEntities?.searchResults || [],
        resultItemCount: searchResults?.searchAcrossEntities?.total || 0,
        error: searchError,
        loading: searchLoading,
        refetch: refetchSearch,
        numResultsPerPage,
        setNumResultsPerPage,
    };

    // Form
    const formInfo = {
        formUrn,
        form,
        isVerificationType,
        formView,
        setFormView,
    };

    // Entity
    const entity = {
        // Entities in Form
        entitiesForForm,

        // Entity Data
        entityData: selectedEntityData as GenericEntityProperties,
        refetch: handleRefetch,

        // Selected Entities
        selectedEntities,
        setSelectedEntities,
        areAllEntitiesSelected,
        setAreAllEntitiesSelected,

        // Selected Entity
        selectedEntity,
        setSelectedEntity,

        // Submitted Entities
        submittedEntities,
        setSubmittedEntities,
        numSubmittedEntities,
        setNumSubmittedEntities,

        // Utils
        isOnEntityProfilePage,
    };

    // Prompt
    const prompts = form ? getBulkByQuestionPrompts(form) : [];
    const promptIndex = (selectedPromptId && prompts.findIndex((p) => p.id === selectedPromptId)) || 0;
    const prompt = {
        // All Prompts
        prompts,

        // Prompt Select
        selectedPromptId,
        setSelectedPromptId,

        // Current Prompt Data
        prompt: prompts.find((p) => p.id === selectedPromptId),
        promptIndex,

        // Bulk utils on a prompt
        displayBulkPromptStyles: formView === FormView.BY_QUESTION,
    };

    // Form States
    const { promptCounts, verificationType, completionType } = counts;
    const isByQuestion = formView === FormView.BY_QUESTION;
    const isBulkVerify = formView === FormView.BULK_VERIFY;
    const noSearchResults = search.resultItemCount === 0;
    const isLastQuestion = promptIndex === prompts.length - 1;
    const states = {
        byQuestion: {
            showFinishRemainingAssets: isByQuestion && noSearchResults && promptCounts.numNotComplete > 0,
            showContinueToVerification:
                isByQuestion &&
                isVerificationType &&
                noSearchResults &&
                promptCounts.numNotComplete === 0 &&
                isLastQuestion,
            showGoToPreviousQuestion:
                isByQuestion &&
                !isVerificationType &&
                noSearchResults &&
                promptCounts.numNotComplete === 0 &&
                isLastQuestion,
            showContinueToNextQuestion:
                isByQuestion && noSearchResults && promptCounts.numNotComplete === 0 && !isLastQuestion,
            showCompleted: isByQuestion && !isVerificationType && completionType.notComplete === 0,
            showVerifyCTAHeader: isByQuestion && isVerificationType && verificationType.verifyReady > 0,
            showVerifyCTA:
                isByQuestion &&
                isVerificationType &&
                verificationType.verifyReady > 0 &&
                verificationType.notVerifyReady === 0,
        },
        bulkVerify: {
            showReturnToQuestions:
                isBulkVerify &&
                noSearchResults &&
                verificationType.verifyReady === 0 &&
                verificationType.notVerifyReady > 0,
            showFinishRemainingAssets: isBulkVerify && noSearchResults && verificationType.verifyReady > 0,
            showCompleted: isBulkVerify && verificationType.verifyReady === 0 && verificationType.notVerifyReady === 0,
        },
    };

    // Bool for form context?
    const isInFormContext = true;

    /*
     * Build & return the provider
     */

    return (
        <EntityFormContext.Provider
            value={{
                isInFormContext,
                loading: isLoading,
                refetch: handleRefetch,
                refetchForBulk,
                shouldRefetch,
                setShouldRefetch,
                submission,
                search,
                form: formInfo,
                filter: {
                    ...filter,
                    shouldClearFilters,
                    setShouldClearFilters,
                },
                entity,
                prompt,
                counts,
                states,
            }}
        >
            {children}
        </EntityFormContext.Provider>
    );
}
