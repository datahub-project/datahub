import React, { useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import { Select } from 'antd';
import { useListMyViewsQuery, useListGlobalViewsQuery } from '../../../../graphql/view.generated';
import { useUserContext } from '../../../context/useUserContext';
import { DataHubView, DataHubViewType } from '../../../../types.generated';
import { ViewBuilder } from '../builder/ViewBuilder';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../utils';
import { PageRoutes } from '../../../../conf/Global';
import { ViewSelectToolTip } from './ViewSelectToolTip';
import { ViewBuilderMode } from '../builder/types';
import { ViewSelectDropdown } from './ViewSelectDropdown';
import { renderViewOptionGroup } from './renderViewOptionGroup';

const selectStyle = {
    width: 240,
};

const dropdownStyle = {
    position: 'fixed',
} as any;

type ViewBuilderDisplayState = {
    mode: ViewBuilderMode;
    visible: boolean;
    view?: DataHubView;
};

const DEFAULT_VIEW_BUILDER_DISPLAY_STATE = {
    mode: ViewBuilderMode.EDITOR,
    visible: false,
    view: undefined,
};

/**
 * The View Select component allows you to select a View to apply to query on the current page. For example,
 * search, recommendations, and browse.
 *
 * The current state of the View select includes an urn that must be forwarded with search, browse, and recommendations
 * requests. As we navigate around the app, the state of the selected View should not change.
 *
 * In the event that a user refreshes their browser, the state of the view should be saved as well.
 */
export const ViewSelect = () => {
    const history = useHistory();
    const userContext = useUserContext();
    const [viewBuilderDisplayState, setViewBuilderDisplayState] = useState<ViewBuilderDisplayState>(
        DEFAULT_VIEW_BUILDER_DISPLAY_STATE,
    );
    const [selectedUrn, setSelectedUrn] = useState<string | undefined>(
        userContext.localState?.selectedViewUrn || undefined,
    );
    const [hoverViewUrn, setHoverViewUrn] = useState<string | undefined>(undefined);

    useEffect(() => {
        setSelectedUrn(userContext.localState?.selectedViewUrn || undefined);
    }, [userContext.localState?.selectedViewUrn, setSelectedUrn]);

    const selectRef = useRef(null);

    /**
     * Queries - Notice, each of these queries is cached. Here we fetch both the user's private views,
     * along with all public views.
     */

    const { data: privateViewsData } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
            viewType: DataHubViewType.Personal,
        },
        fetchPolicy: 'cache-first',
    });

    // Fetch Public Views
    const { data: publicViewsData } = useListGlobalViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    /**
     * Event Handlers
     */

    const onSelectView = (newUrn) => {
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: newUrn,
        });
    };

    const onClickCreateView = () => {
        setViewBuilderDisplayState({
            visible: true,
            mode: ViewBuilderMode.EDITOR,
            view: undefined,
        });
    };

    const onClickEditView = (view) => {
        setViewBuilderDisplayState({
            visible: true,
            mode: ViewBuilderMode.EDITOR,
            view,
        });
    };

    const onCloseViewBuilder = () => {
        setViewBuilderDisplayState(DEFAULT_VIEW_BUILDER_DISPLAY_STATE);
    };

    const onClickPreviewView = (view) => {
        setViewBuilderDisplayState({
            visible: true,
            mode: ViewBuilderMode.PREVIEW,
            view,
        });
    };

    const onClear = () => {
        setSelectedUrn(undefined);
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
    };

    const onClickManageViews = () => {
        history.push(PageRoutes.SETTINGS_VIEWS);
    };

    /**
     * Render variables
     */
    const privateViews = privateViewsData?.listMyViews?.views || [];
    const publicViews = publicViewsData?.listGlobalViews?.views || [];
    const privateViewCount = privateViews?.length || 0;
    const publicViewCount = publicViews?.length || 0;
    const hasViews = privateViewCount > 0 || publicViewCount > 0 || false;

    /**
     * Notice - we assume that we will find the selected View urn in the list
     * of Views retrieved for the user (private or public). If this is not the case,
     * the view will be set to undefined.
     *
     * This may become a problem if a list of public views exceeds the default pagination size of 1,000.
     */
    const foundSelectedUrn =
        (privateViews.filter((view) => view.urn === selectedUrn)?.length || 0) > 0 ||
        (publicViews.filter((view) => view.urn === selectedUrn)?.length || 0) > 0 ||
        false;

    return (
        <>
            <ViewSelectToolTip visible={selectedUrn === undefined}>
                <Select
                    data-testid="view-select"
                    style={selectStyle}
                    onChange={() => (selectRef?.current as any)?.blur()}
                    value={(foundSelectedUrn && selectedUrn) || undefined}
                    placeholder="Select a View"
                    onSelect={onSelectView}
                    onClear={onClear}
                    allowClear
                    ref={selectRef}
                    optionLabelProp="label"
                    dropdownStyle={dropdownStyle}
                    dropdownRender={(menu) => (
                        <ViewSelectDropdown
                            menu={menu}
                            hasViews={hasViews}
                            onClickCreateView={onClickCreateView}
                            onClickClear={onClear}
                            onClickManageViews={onClickManageViews}
                        />
                    )}
                >
                    {privateViewCount > 0 &&
                        renderViewOptionGroup({
                            views: privateViews,
                            label: 'Private',
                            isOwnedByUser: true,
                            userContext,
                            hoverViewUrn,
                            setHoverViewUrn,
                            onClickEditView,
                            onClickPreviewView,
                        })}
                    {publicViewCount > 0 &&
                        renderViewOptionGroup({
                            views: publicViews,
                            label: 'Public',
                            userContext,
                            hoverViewUrn,
                            setHoverViewUrn,
                            onClickEditView,
                            onClickPreviewView,
                        })}
                </Select>
            </ViewSelectToolTip>
            {viewBuilderDisplayState.visible && (
                <ViewBuilder
                    urn={viewBuilderDisplayState.view?.urn || undefined}
                    initialState={viewBuilderDisplayState.view}
                    mode={viewBuilderDisplayState.mode}
                    onSubmit={onCloseViewBuilder}
                    onCancel={onCloseViewBuilder}
                />
            )}
        </>
    );
};
