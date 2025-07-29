import { FetchPolicy } from '@apollo/client';
import { useMemo } from 'react';

import { ActionWorkflowFragment, useListActionWorkflowsQuery } from '@graphql/actionWorkflow.generated';
import { ActionWorkflowCategory, ActionWorkflowEntrypointType, EntityType, ListActionWorkflowsInput } from '@types';

export type WorkflowContext = {
    entrypointType: ActionWorkflowEntrypointType;
    entityType?: EntityType;
    entityUrn?: string;
};

export type UseListActionWorkflowsOptions = {
    context: WorkflowContext;
    category?: ActionWorkflowCategory;
    customCategory?: string;
    enabled?: boolean;
    fetchPolicy?: FetchPolicy;
};

/**
 * Hook to list action workflows based on current page context
 *
 * @param options Configuration options for filtering workflows
 * @returns Query result with workflows and loading state
 */
export const useListActionWorkflows = (options: UseListActionWorkflowsOptions) => {
    const { context, category, customCategory, enabled = true, fetchPolicy = 'cache-first' } = options;

    const input: ListActionWorkflowsInput = useMemo(
        () => ({
            start: 0,
            count: 1000, // Get all workflows for now
            entrypointType: context.entrypointType,
            ...(category && { category }),
            ...(customCategory && { customCategory }),
            ...(context.entityType && { entityType: context.entityType }),
        }),
        [context.entrypointType, context.entityType, category, customCategory],
    );

    const { data, loading, error, refetch } = useListActionWorkflowsQuery({
        variables: { input },
        skip: !enabled,
        fetchPolicy,
    });

    const workflows = data?.listActionWorkflows?.workflows || [];
    const total = data?.listActionWorkflows?.total || 0;

    return {
        workflows,
        total,
        loading,
        error,
        refetch,
    };
};

/**
 * Helper function to create workflow context for the home page
 */
export const getHomePageWorkflowContext = (): WorkflowContext => ({
    entrypointType: ActionWorkflowEntrypointType.Home,
});

/**
 * Helper to extract the label for the workflow entrypoint
 */
export const getEntryPointLabel = (workflow: ActionWorkflowFragment, context: WorkflowContext): string => {
    const { entrypointType } = context;
    const workflowEntrypoint = workflow?.trigger?.form?.entrypoints?.find((ep) => ep.type === entrypointType);
    return workflowEntrypoint?.label || workflow.name;
};

/**
 * Helper function to create workflow context for entity profile pages
 */
export const getEntityProfileWorkflowContext = (entityType: EntityType, entityUrn?: string): WorkflowContext => ({
    entrypointType: ActionWorkflowEntrypointType.EntityProfile,
    entityType,
    entityUrn,
});
