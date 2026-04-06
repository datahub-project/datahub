import { useApolloClient } from '@apollo/client';
import { Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { removeFromListMyViewsCache, removeFromViewSelectCaches } from '@app/entityV2/view/cacheUtils';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';
import { Menu, colors, notification } from '@src/alchemy-components';
import { MenuItemType } from '@src/alchemy-components/components/Menu/types';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useUpdateGlobalViewsSettingsMutation } from '@graphql/app.generated';
import { useUpdateCorpUserViewsSettingsMutation } from '@graphql/user.generated';
import { useDeleteViewMutation } from '@graphql/view.generated';
import { DataHubView, DataHubViewType } from '@types';

const MenuTrigger = styled.div<{ $visible?: boolean; $isShowNavBarRedesign?: boolean }>`
    display: ${(props) => (props.$visible ? 'flex' : 'none')};
    align-items: center;
    justify-content: center;
    cursor: pointer;
    width: 20px;
    color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1800] : 'inherit')};
    font-size: 18px;

    &:hover {
        color: ${colors.gray[600]};
    }
`;

const DEFAULT_VIEW_BUILDER_STATE = {
    mode: ViewBuilderMode.EDITOR,
    visible: false,
};

type Props = {
    view: DataHubView;
    visible?: boolean;
    isOwnedByUser?: boolean;
    trigger?: 'hover' | 'click';
    onClickEdit?: () => void;
    onClickPreview?: () => void;
    onClickDelete?: () => void;
    selectView?: () => void;
};

export const ViewDropdownMenu = ({
    view,
    visible,
    isOwnedByUser,
    trigger = 'hover',
    onClickEdit,
    onClickPreview,
    onClickDelete,
    selectView,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const userContext = useUserContext();
    const client = useApolloClient();

    const [updateUserViewSettingMutation] = useUpdateCorpUserViewsSettingsMutation();
    const [updateGlobalViewSettingMutation] = useUpdateGlobalViewsSettingsMutation();
    const [deleteViewMutation] = useDeleteViewMutation();

    const [viewBuilderState, setViewBuilderState] = useState(DEFAULT_VIEW_BUILDER_STATE);

    const setUserDefault = (viewUrn: string | null) => {
        updateUserViewSettingMutation({
            variables: {
                input: {
                    defaultView: viewUrn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    if (viewUrn && selectView) selectView();
                    userContext.updateState({
                        ...userContext.state,
                        views: {
                            ...userContext.state.views,
                            personalDefaultViewUrn: viewUrn,
                        },
                    });
                    analytics.event({
                        type: EventType.SetUserDefaultViewEvent,
                        urn: viewUrn,
                        viewType: (viewUrn && view.viewType) || null,
                    });
                }
            })
            .catch(() => {
                notification.error({
                    message: 'Failed to update default view',
                    description: 'An unexpected error occurred.',
                    duration: 3,
                });
            });
    };

    const setGlobalDefault = (viewUrn: string | null) => {
        updateGlobalViewSettingMutation({
            variables: {
                input: {
                    defaultView: viewUrn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    userContext.updateState({
                        ...userContext.state,
                        views: {
                            ...userContext.state.views,
                            globalDefaultViewUrn: viewUrn,
                        },
                    });
                    analytics.event({
                        type: EventType.SetGlobalDefaultViewEvent,
                        urn: viewUrn,
                    });
                }
            })
            .catch(() => {
                notification.error({
                    message: "Failed to update organization's default view",
                    description: 'An unexpected error occurred.',
                    duration: 3,
                });
            });
    };

    const onEditView = () => {
        if (onClickEdit) {
            onClickEdit();
        } else {
            setViewBuilderState({ mode: ViewBuilderMode.EDITOR, visible: true });
        }
    };

    const onPreviewView = () => {
        if (onClickPreview) {
            onClickPreview();
        } else {
            setViewBuilderState({ mode: ViewBuilderMode.PREVIEW, visible: true });
        }
    };

    const onViewBuilderClose = () => {
        setViewBuilderState(DEFAULT_VIEW_BUILDER_STATE);
    };

    const deleteView = (viewUrn: string) => {
        deleteViewMutation({ variables: { urn: viewUrn } })
            .then(({ errors }) => {
                if (!errors) {
                    removeFromViewSelectCaches(viewUrn, client);
                    removeFromListMyViewsCache(viewUrn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined, undefined);
                    if (viewUrn === userContext.localState?.selectedViewUrn) {
                        userContext.updateLocalState({
                            ...userContext.localState,
                            selectedViewUrn: undefined,
                        });
                    }
                    notification.success({
                        message: 'View removed',
                        duration: 2,
                    });
                }
            })
            .catch(() => {
                notification.error({
                    message: 'Failed to delete View',
                    description: 'An unexpected error occurred.',
                    duration: 3,
                });
            });
    };

    const confirmDeleteView = () => {
        if (onClickDelete) {
            onClickDelete();
        } else {
            Modal.confirm({
                title: `Confirm Remove ${view.name}`,
                content: 'Are you sure you want to remove this View?',
                onOk() {
                    deleteView(view.urn);
                },
                onCancel() {},
                okText: 'Yes',
                maskClosable: true,
                closable: true,
            });
        }
    };

    const canManageGlobalViews = userContext.platformPrivileges?.manageGlobalViews;
    const canManageView = isOwnedByUser || canManageGlobalViews;

    const maybePersonalDefaultViewUrn = userContext.state?.views?.personalDefaultViewUrn;
    const maybeGlobalDefaultViewUrn = userContext.state?.views?.globalDefaultViewUrn;

    const isGlobalView = view.viewType === DataHubViewType.Global;
    const isUserDefault = view.urn === maybePersonalDefaultViewUrn;
    const isGlobalDefault = view.urn === maybeGlobalDefaultViewUrn;

    const showRemoveGlobalDefaultView = canManageGlobalViews && isGlobalView && isGlobalDefault;
    const showSetGlobalDefaultView = canManageGlobalViews && isGlobalView && !isGlobalDefault;

    const menuItems: MenuItemType[] = [];

    if (canManageView) {
        menuItems.push({
            type: 'item',
            key: 'edit',
            title: 'Edit',
            tooltip: 'Edit this View',
            onClick: onEditView,
        });
    } else {
        menuItems.push({
            type: 'item',
            key: 'preview',
            title: 'Preview',
            tooltip: 'See the View definition',
            onClick: onPreviewView,
        });
    }

    if (isUserDefault) {
        menuItems.push({
            type: 'item',
            key: 'remove-default',
            title: 'Remove as default',
            tooltip: 'Remove this View as your personal default',
            onClick: () => setUserDefault(null),
        });
    } else {
        menuItems.push({
            type: 'item',
            key: 'set-default',
            title: 'Make my default',
            tooltip: 'Make this View your personal default',
            onClick: () => setUserDefault(view.urn),
        });
    }

    if (showRemoveGlobalDefaultView) {
        menuItems.push({
            type: 'item',
            key: 'remove-global-default',
            title: 'Remove organization default',
            tooltip: "Remove this View as your organization's default",
            onClick: () => setGlobalDefault(null),
        });
    }

    if (showSetGlobalDefaultView) {
        menuItems.push({
            type: 'item',
            key: 'set-global-default',
            title: 'Make organization default',
            tooltip: "Make this View your organization's default",
            onClick: () => setGlobalDefault(view.urn),
        });
    }

    if (canManageView) {
        menuItems.push({
            type: 'item',
            key: 'delete',
            title: 'Delete',
            tooltip: 'Delete this View',
            danger: true,
            onClick: confirmDeleteView,
        });
    }

    const handleClick = (e: React.MouseEvent) => {
        e.stopPropagation();
    };

    return (
        <>
            <Menu items={menuItems} trigger={[trigger]}>
                <MenuTrigger
                    data-testid="views-table-dropdown"
                    $visible={visible}
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                    onClick={handleClick}
                    role="button"
                    tabIndex={0}
                >
                    &#8942;
                </MenuTrigger>
            </Menu>
            {viewBuilderState.visible && (
                <ViewBuilder
                    mode={viewBuilderState.mode}
                    urn={view.urn}
                    initialState={view}
                    onSubmit={onViewBuilderClose}
                    onCancel={onViewBuilderClose}
                />
            )}
        </>
    );
};
