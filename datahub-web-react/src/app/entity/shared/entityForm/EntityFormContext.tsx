import React, { useContext } from 'react';

import { AndFilterInput, Entity, Form, FormFilter, FormPrompt, SearchResult, Test } from '../../../../types.generated';
import { EntityAndType, GenericEntityProperties } from '../types';
import { SearchForEntitiesByFormQuery } from '../../../../graphql/form.generated';

export enum FormView {
    BY_ENTITY,
    BY_QUESTION,
    BULK_VERIFY,
}

export const FORM_RESPONSES_FILTER = 'formResponsesFilter';

export enum FormResponsesFilter {
    INCOMPLETE = 'INCOMPLETE',
    COMPLETE = 'COMPLETE',
}

export type EntityFormContextType = {
    isInFormContext: boolean;
    loading: boolean;
    refetch: () => Promise<any>;
    refetchForBulk: () => void;
    shouldRefetch: boolean;
    setShouldRefetch: (bool: boolean) => void;
    submission: {
        nonOptimisticLoading: boolean;
        verificationDataLoading: boolean;
        handlePromptSubmission: (promptId: string, entityUrns: string[]) => void;
        handleUndoPromptSubmission: (promptId: string, entityUrns: string[]) => void;
        handleBulkVerifySubmission: (entityUrns: string[]) => void;
        activeTasks: Test[];
        completeTasks: Test[];
        handleAsyncBatchSubmit: (taskUrn: string, promptId: string) => void;
    };
    search: {
        results: SearchForEntitiesByFormQuery;
        resultItems: SearchResult[];
        resultItemCount: number;
        error?: any;
        loading: boolean;
        refetch: () => void;
        numResultsPerPage: number;
        setNumResultsPerPage: (num: number) => void;
    };
    form: {
        formUrn: string;
        form?: Form;
        isVerificationType: boolean;
        formView: FormView;
        setFormView: (formView: FormView) => void;
    };
    filter: {
        formFilter?: FormFilter;
        formResponsesFilters?: FormResponsesFilter[];
        orFilters?: AndFilterInput[] | null;
        setFormResponsesFilters: (value: FormResponsesFilter[]) => void;
        shouldClearFilters: boolean;
        setShouldClearFilters: (bool: boolean) => void;
    };
    entity: {
        entitiesForForm?: EntityAndType[];
        entityData?: GenericEntityProperties;
        refetch: () => Promise<any>;
        areAllEntitiesSelected: boolean;
        setAreAllEntitiesSelected: (areAllSelected: boolean) => void;
        selectedEntities: EntityAndType[];
        setSelectedEntities: (entities: EntityAndType[]) => void;
        selectedEntity?: Entity;
        setSelectedEntity: (sortOption: Entity) => void;
        submittedEntities: EntityAndType[];
        setSubmittedEntities: (entities: EntityAndType[]) => void;
        numSubmittedEntities: number;
        setNumSubmittedEntities: (count: number) => void;
        isOnEntityProfilePage?: boolean;
    };
    prompt: {
        prompts?: FormPrompt[];
        selectedPromptId?: string;
        setSelectedPromptId: (promptId: string) => void;
        prompt?: FormPrompt;
        promptIndex: number;
        displayBulkPromptStyles: boolean;
    };
    counts: {
        verificationType: {
            verifyReady: number;
            notVerifyReady: number;
        };
        completionType: {
            notComplete: number;
        };
        promptCounts: {
            numNotComplete: number;
            numComplete: number;
        };
    };
    states: {
        byQuestion: {
            showFinishRemainingAssets: boolean; // some assets for prompt have response
            showContinueToNextQuestion: boolean; // all assets for prompt have response
            showGoToPreviousQuestion: boolean; // done with final question for completion form and form is not done
            showContinueToVerification: boolean; // done with final question for verification form
            showCompleted: boolean; // all assets have response for all prompts, NOT verify form type
            showVerifyCTAHeader: boolean; // at least 1 asset has a response is ready for verification
            showVerifyCTA: boolean; // all assets have response to all prompts & ready for verification
        };
        bulkVerify: {
            showReturnToQuestions: boolean; // all ready assets verified, need to finish other assets not ready
            showFinishRemainingAssets: boolean; // some assets are verified
            showCompleted: boolean; // all assets are verified
        };
    };
};

export const DEFAULT_CONTEXT = {
    isInFormContext: false,
    loading: true,
    refetch: () => Promise.resolve({}),
    refetchForBulk: () => null,
    shouldRefetch: false,
    setShouldRefetch: (_: boolean) => null,
    submission: {
        nonOptimisticLoading: false,
        verificationDataLoading: false,
        handlePromptSubmission: (_: string, __: string[]) => null,
        handleUndoPromptSubmission: (_: string, __: string[]) => null,
        handleBulkVerifySubmission: (_: string[]) => null,
        activeTasks: [],
        completeTasks: [],
        handleAsyncBatchSubmit: (_taskId: string, _promptId: string) => {},
    },
    search: {
        results: {},
        resultItems: [],
        resultItemCount: 0,
        error: undefined,
        loading: false,
        refetch: () => Promise.resolve({}),
        numResultsPerPage: 10,
        setNumResultsPerPage: (_: number) => null,
    },
    form: {
        formUrn: '',
        form: undefined,
        isVerificationType: true,
        formView: FormView.BY_ENTITY,
        setFormView: (_: FormView) => null,
    },
    filter: {
        formFilter: undefined,
        formResponsesFilters: undefined,
        orFilters: undefined,
        setFormResponsesFilters: () => null,
        shouldClearFilters: false,
        setShouldClearFilters: (_: boolean) => null,
    },
    entity: {
        entitiesForForm: undefined,
        entityData: undefined,
        refetch: () => Promise.resolve({}),
        areAllEntitiesSelected: false,
        setAreAllEntitiesSelected: (_: boolean) => null,
        selectedEntities: [],
        setSelectedEntities: (_: EntityAndType[]) => null,
        selectedEntity: undefined,
        setSelectedEntity: (_: Entity) => null,
        submittedEntities: [],
        setSubmittedEntities: (_: EntityAndType[]) => null,
        numSubmittedEntities: 0,
        setNumSubmittedEntities: (_: number) => null,
        isOnEntityProfilePage: undefined,
    },
    prompt: {
        prompts: undefined,
        selectedPromptId: undefined,
        setSelectedPromptId: (_: string) => null,
        prompt: undefined,
        promptIndex: 0,
        displayBulkPromptStyles: false,
    },
    counts: {
        verificationType: {
            verifyReady: 0,
            notVerifyReady: 0,
        },
        completionType: {
            notComplete: 0,
        },
        promptCounts: {
            numNotComplete: 0,
            numComplete: 0,
        },
    },
    states: {
        byQuestion: {
            showFinishRemainingAssets: false,
            showContinueToNextQuestion: false,
            showGoToPreviousQuestion: false,
            showContinueToVerification: false,
            showCompleted: false,
            showVerifyCTAHeader: false,
            showVerifyCTA: false,
        },
        bulkVerify: {
            showReturnToQuestions: false,
            showFinishRemainingAssets: false,
            showCompleted: false,
        },
    },
};

export const EntityFormContext = React.createContext<EntityFormContextType>(DEFAULT_CONTEXT);

export function useEntityFormContext() {
    const context = useContext(EntityFormContext);
    if (context === null)
        throw new Error(`${useEntityFormContext.name} must be used under a EntityFormContextProvider`);
    return context;
}
