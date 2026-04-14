import { useApolloClient } from '@apollo/client';
import React from 'react';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ViewBuilderModal } from '@app/entityV2/view/builder/ViewBuilderModal';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { updateListMyViewsCache, updateViewSelectCache } from '@app/entityV2/view/cacheUtils';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE, convertStateToUpdateInput } from '@app/entityV2/view/utils';
import { useSearchVersion } from '@app/search/useSearchAndBrowseVersion';
import { notification } from '@src/alchemy-components';

import { useCreateViewMutation, useUpdateViewMutation } from '@graphql/view.generated';
import { DataHubView } from '@types';

type Props = {
    mode: ViewBuilderMode;
    urn?: string | null; // When editing an existing view, this is provided.
    initialState?: ViewBuilderState;
    onSubmit?: (state: ViewBuilderState) => void;
    onCancel?: () => void;
};

/**
 * This component handles creating and editing DataHub Views.
 */
export const ViewBuilder = ({ mode, urn, initialState, onSubmit, onCancel }: Props) => {
    const searchVersion = useSearchVersion();
    const userContext = useUserContext();

    const client = useApolloClient();
    const [updateViewMutation] = useUpdateViewMutation();
    const [createViewMutation] = useCreateViewMutation();

    const emitTrackingEvent = (viewUrn: string, state: ViewBuilderState, isCreate: boolean) => {
        const filterFields = Array.from(new Set(state.definition?.filter?.filters.map((filter) => filter.field) ?? []));
        const entityTypes = Array.from(new Set(state.definition?.entityTypes ?? []));

        analytics.event({
            type: isCreate ? EventType.CreateViewEvent : EventType.UpdateViewEvent,
            urn: viewUrn,
            viewType: state.viewType,
            filterFields,
            entityTypes,
            searchVersion,
        });
    };

    /**
     * Updates the selected View to a new urn. When a View is created or edited,
     * we automatically update the currently applied View to be that view.
     */
    const updatedSelectedView = (viewUrn: string) => {
        // Hack: Force a re-render of the pages dependent on the value of the currently selected View
        // (e.g. Search results)
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: viewUrn,
        });
    };

    const addViewToCaches = (viewUrn: string, view: DataHubView) => {
        updateListMyViewsCache(viewUrn, view, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined, undefined);
        updateViewSelectCache(viewUrn, view, client);
    };

    const updateViewInCaches = (viewUrn: string, view: DataHubView) => {
        updateListMyViewsCache(viewUrn, view, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined, undefined);
        updateViewSelectCache(viewUrn, view, client);
    };

    /**
     * Updates the caches that power the Manage My Views and Views Select
     * experiences when a new View has been create or an existing View has been updated.
     */
    const updateViewCaches = (viewUrn: string, view: DataHubView, isCreate: boolean) => {
        if (isCreate) {
            addViewToCaches(viewUrn, view);
        } else {
            updateViewInCaches(viewUrn, view);
        }
    };

    /**
     * Create a new View, or update an existing one.
     *
     * @param state the state, which defines the fields of the new View.
     */
    const upsertView = (state: ViewBuilderState) => {
        const viewInput = convertStateToUpdateInput(state);
        const isCreate = urn === undefined;
        const mutation = isCreate ? createViewMutation : updateViewMutation;
        const variables = urn ? { urn, input: { ...viewInput, viewType: undefined } } : { input: viewInput };
        (
            mutation({
                variables: variables as any,
            }) as any
        )
            .then((res) => {
                const updatedView = isCreate ? res.data?.createView : res.data?.updateView;
                emitTrackingEvent(updatedView.urn, updatedView, isCreate);
                updateViewCaches(updatedView.urn, updatedView, isCreate);
                updatedSelectedView(updatedView.urn);
                onSubmit?.(state);
            })
            .catch((_) => {
                notification.error({
                    message: 'Failed to save View',
                    description: 'An unexpected error occurred.',
                    duration: 3,
                });
            });
    };

    return (
        <ViewBuilderModal
            mode={mode}
            urn={urn || undefined}
            initialState={initialState}
            onSubmit={upsertView}
            onCancel={onCancel}
        />
    );
};
