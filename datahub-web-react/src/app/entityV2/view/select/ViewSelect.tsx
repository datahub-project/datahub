import { Popover, colors } from '@components';
import { debounce } from 'lodash';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { HALF_SECOND_IN_MS, MAX_ROWS_BEFORE_DEBOUNCE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { ViewSelectPopoverContent } from '@app/entityV2/view/select/ViewSelectPopoverContent';
import { renderSelectedView } from '@app/entityV2/view/select/renderSelectedView';
import { renderViewOptionGroup } from '@app/entityV2/view/select/renderViewOptionGroup';
import '@app/entityV2/view/select/style.css';
import { filterViews } from '@app/entityV2/view/select/utils';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';
import { PageRoutes } from '@conf/Global';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubView, DataHubViewType } from '@types';

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

const ViewSelectContainer = styled.div`
    &&& {
        display: flex;
        align-items: center;
        padding: 0px 0px;

        & .close-container {
            position: absolute;
            top: -10px;
            right: -5px;
            background-color: ${ANTD_GRAY[1]};
            display: flex;
            align-items: center;
            border-radius: 100%;
            padding: 5px;
        }

        .ant-select {
            .ant-select-selection-search {
                position: absolute;
            }

            &.ant-select-open {
                .ant-select-selection-placeholder,
                .ant-select-selection-item {
                    color: ${ANTD_GRAY[1]};
                }
            }

            &:not(.ant-select-open) {
                .ant-select-selection-placeholder,
                .ant-select-selection-item {
                    color: #fff;
                }
            }

            .ant-select-selection-placeholder {
                display: flex;
                align-items: center;
                justify-content: center;
                height: 100%;
            }

            .ant-select-selection-item {
                font-weight: 700;
                font-size: 14px;
                text-align: right;
                padding: 0px;
            }
        }
    }
`;

const overlayInnerStyle = {
    background: 'transparent',
    display: 'flex',
    width: '100%',
};

const getOverlayInnerStyle = (isShowNavBarRedesign?: boolean) => {
    if (isShowNavBarRedesign)
        return {
            display: 'flex',
            width: '100%',
            opacity: 0.97,
            backgroundColor: colors.gray[1600],
            borderRadius: '0 0 12px 12px',
            paddingTop: '1px',
            boxShadow: '0px 525px 20px 500px rgba(0, 0, 0, 0.12), 0px 65px 60px 0px rgba(0, 0, 0, 0.12)',
        };

    return overlayInnerStyle;
};

const overlayStyle = {
    left: '0px',
    backgroundColor: REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK,
    backdropFilter: 'blur(5px)',
    opacity: 0.97,
    zIndex: 13,
    'transform-origin': '0',
};

const getOverlayStyle = (isShowNavBarRedesign?: boolean) => {
    if (isShowNavBarRedesign)
        return {
            left: '0px',
            zIndex: 13,
            paddingTop: '5px',
            'transform-origin': '0',
        };

    return overlayStyle;
};

const Blur = styled.div<{ $isOpen?: boolean }>`
    position: absolute;
    top: 69px;
    left: 0;
    width: 100%;
    height: calc(100vh - 69px);
    z-index: 12;
    backdrop-filter: blur(2px);
    ${(props) => !props.$isOpen && 'display: none;'}
`;

interface Props {
    isOpen?: boolean;
    onOpenChange?: (isOpen: boolean) => void;
}

/**
 * The View Select component allows you to select a View to apply to query on the current page. For example,
 * search, recommendations, and browse.
 *
 * The current state of the View select includes an urn that must be forwarded with search, browse, and recommendations
 * requests. As we navigate around the app, the state of the selected View should not change.
 *
 * In the event that a user refreshes their browser, the state of the view should be saved as well.
 */
export const ViewSelect = ({ isOpen, onOpenChange }: Props) => {
    const history = useHistory();
    const userContext = useUserContext();
    const [viewBuilderDisplayState, setViewBuilderDisplayState] = useState<ViewBuilderDisplayState>(
        DEFAULT_VIEW_BUILDER_DISPLAY_STATE,
    );
    const [selectedUrn, setSelectedUrn] = useState<string | undefined>(
        userContext.localState?.selectedViewUrn || undefined,
    );
    const [hoverViewUrn, setHoverViewUrn] = useState<string | undefined>(undefined);
    const [privateView, setPrivateView] = useState<boolean>(true);
    const [publicView, setPublicView] = useState<boolean>(true);

    const [filterText, setFilterText] = useState('');
    const [isInternalOpen, setIsInternalOpen] = useState(!!isOpen);
    const [selectedViewName, setSelectedView] = useState<string>('');

    const isShowNavBarRedesign = useShowNavBarRedesign();

    const selectRef = useRef(null);

    const scrollToRef = useRef<HTMLDivElement>(null);
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

    // Possibility to control open/close state by parents components
    useEffect(() => {
        if (isOpen !== undefined) setIsInternalOpen(isOpen);
    }, [isOpen]);

    useEffect(() => {
        setSelectedUrn(userContext.localState?.selectedViewUrn || undefined);
        const selectedView =
            privateViewsData?.listMyViews?.views?.find(
                (view) => view?.urn === userContext.localState?.selectedViewUrn,
            ) ||
            publicViewsData?.listGlobalViews?.views?.find(
                (view) => view?.urn === userContext.localState?.selectedViewUrn,
            );
        if (selectedView === undefined) {
            setSelectedView('');
        } else {
            setSelectedView(selectedView.name);
        }
    }, [userContext.localState?.selectedViewUrn, setSelectedUrn, privateViewsData, publicViewsData]);

    const highlightedPublicViewData = filterViews(filterText, publicViewsData?.listGlobalViews?.views || []);
    const highlightedPrivateViewData = filterViews(filterText, privateViewsData?.listMyViews?.views || []);

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        (highlightedPublicViewData.length || highlightedPrivateViewData.length) > MAX_ROWS_BEFORE_DEBOUNCE
            ? HALF_SECOND_IN_MS
            : 0,
    );

    /**
     * Event Handlers
     */

    const updateOpenState = useCallback(
        (newIsOpen: boolean) => {
            if (isOpen === undefined) setIsInternalOpen(newIsOpen);
            onOpenChange?.(newIsOpen);
        },
        [onOpenChange, isOpen],
    );

    const onSelectView = (newUrn) => {
        const selectedView =
            highlightedPrivateViewData?.find((view) => view?.urn === selectedUrn) ||
            highlightedPublicViewData?.find((view) => view?.urn === selectedUrn);
        setSelectedView(selectedView?.name ?? '');
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: newUrn,
        });
        setTimeout(() => {
            updateOpenState(false);
        }, 250);
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
        setSelectedView('');
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
        updateOpenState(false);
    };

    const onClickManageViews = () => {
        history.push(PageRoutes.SETTINGS_VIEWS);
        updateOpenState(false);
    };

    const onClickViewTypeFilter = (type: string) => {
        setPrivateView(type === 'private' || type === 'all');
        setPublicView(type === 'public' || type === 'all');
    };

    const onOpenChangeHandler = useCallback(() => {
        scrollToRef?.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
        updateOpenState(!isInternalOpen);
    }, [isInternalOpen, updateOpenState]);

    /**
     * Render variables
     */
    const privateViews = highlightedPrivateViewData || [];
    const publicViews = highlightedPublicViewData || [];
    const privateViewCount = privateViews?.length || 0;
    const publicViewCount = publicViews?.length || 0;
    const hasViews = privateViewCount > 0 || publicViewCount > 0 || false;

    return (
        <>
            {isShowNavBarRedesign && createPortal(<Blur $isOpen={isInternalOpen} />, document.body)}
            <ViewSelectContainer>
                <Popover
                    open={isInternalOpen}
                    onOpenChange={onOpenChangeHandler}
                    content={
                        <>
                            <ViewSelectPopoverContent
                                privateView={privateView}
                                publicView={publicView}
                                onClickCreateView={onClickCreateView}
                                onClickManageViews={onClickManageViews}
                                onClickViewTypeFilter={onClickViewTypeFilter}
                                onChangeSearch={debouncedSetFilterText}
                            >
                                {hasViews &&
                                    privateViewCount > 0 &&
                                    privateView &&
                                    renderViewOptionGroup({
                                        selectedUrn,
                                        views: highlightedPrivateViewData,
                                        isOwnedByUser: true,
                                        userContext,
                                        hoverViewUrn,
                                        scrollToRef,
                                        setHoverViewUrn,
                                        onClickEditView,
                                        onClickPreviewView,
                                        onClickClear: onClear,
                                        onSelectView,
                                    })}
                                {hasViews &&
                                    publicViewCount > 0 &&
                                    publicView &&
                                    renderViewOptionGroup({
                                        selectedUrn,
                                        views: highlightedPublicViewData,
                                        userContext,
                                        hoverViewUrn,
                                        scrollToRef,
                                        setHoverViewUrn,
                                        onClickEditView,
                                        onClickPreviewView,
                                        onClickClear: onClear,
                                        onSelectView,
                                    })}
                            </ViewSelectPopoverContent>
                        </>
                    }
                    trigger="click"
                    overlayClassName="view-select-popover"
                    overlayInnerStyle={getOverlayInnerStyle(isShowNavBarRedesign)}
                    overlayStyle={getOverlayStyle(isShowNavBarRedesign)}
                    showArrow={false}
                    popupVisible={false}
                    ref={selectRef}
                >
                    {renderSelectedView({ selectedViewName, onClear, isShowNavBarRedesign })}
                </Popover>
                {viewBuilderDisplayState.visible && (
                    <ViewBuilder
                        urn={viewBuilderDisplayState.view?.urn || undefined}
                        initialState={viewBuilderDisplayState.view}
                        mode={viewBuilderDisplayState.mode}
                        onSubmit={onCloseViewBuilder}
                        onCancel={onCloseViewBuilder}
                    />
                )}
            </ViewSelectContainer>
        </>
    );
};
