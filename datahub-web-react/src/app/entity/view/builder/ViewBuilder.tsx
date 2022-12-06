import React from 'react';
import { message } from 'antd';
import { useApolloClient } from '@apollo/client';
import { useCreateViewMutation, useUpdateViewMutation } from '../../../../graphql/view.generated';
import { ViewBuilderState } from '../types';
import { ViewBuilderModal } from './ViewBuilderModal';
import { updateViewSelectCache, updateListMyViewsCache } from '../cacheUtils';
import { convertStateToUpdateInput, DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../utils';
import { useUserContext } from '../../../context/useUserContext';
import { ViewBuilderMode } from './types';
import analytics, { Event, EventType } from '../../../analytics';
import { DataHubView } from '../../../../types.generated';

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
    const userContext = useUserContext();

    const client = useApolloClient();
    const [updateViewMutation] = useUpdateViewMutation();
    const [createViewMutation] = useCreateViewMutation();

    const emitTrackingEvent = (viewUrn: string, state: ViewBuilderState, isCreate: boolean) => {
        analytics.event({
            type: isCreate ? EventType.CreateViewEvent : EventType.UpdateViewEvent,
            urn: viewUrn,
            viewType: state.viewType,
        } as Event);
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
        const variables = urn
            ? { urn, input: { ...viewInput, viewType: undefined } }
            : {
                  input: viewInput,
              };
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
                message.destroy();
                message.error({
                    content: `Failed to save View! An unexpected error occurred.`,
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
