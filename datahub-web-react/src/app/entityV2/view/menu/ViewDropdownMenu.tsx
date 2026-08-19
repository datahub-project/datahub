import { useApolloClient } from '@apollo/client';
import { Modal } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { removeFromListMyViewsCache, removeFromViewSelectCaches } from '@app/entityV2/view/cacheUtils';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';
import { Menu, notification } from '@src/alchemy-components';
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
    color: ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.text : 'inherit')};
    font-size: 18px;

    &:hover {
        color: ${(props) => props.theme.colors.icon};
    }
`;

const VERTICAL_ELLIPSIS = '⋮';

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
    const { t } = useTranslation('entity.views');
    const { t: tc } = useTranslation('common.actions');
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
                    message: t('updateDefaultError'),
                    description: t('errorDescription'),
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
                    message: t('updateOrgDefaultError'),
                    description: t('errorDescription'),
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
                        message: t('deleteSuccess'),
                        duration: 2,
                    });
                }
            })
            .catch(() => {
                notification.error({
                    message: t('deleteError'),
                    description: t('errorDescription'),
                    duration: 3,
                });
            });
    };

    const confirmDeleteView = () => {
        if (onClickDelete) {
            onClickDelete();
        } else {
            Modal.confirm({
                title: t('deleteConfirm.title', { name: view.name }),
                content: t('deleteConfirm.content'),
                onOk() {
                    deleteView(view.urn);
                },
                onCancel() {},
                okText: tc('yes'),
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
            title: tc('edit'),
            tooltip: t('menu.editTooltip'),
            onClick: onEditView,
        });
    } else {
        menuItems.push({
            type: 'item',
            key: 'preview',
            title: tc('preview'),
            tooltip: t('menu.previewTooltip'),
            onClick: onPreviewView,
        });
    }

    if (isUserDefault) {
        menuItems.push({
            type: 'item',
            key: 'remove-default',
            title: t('menu.removeDefault'),
            tooltip: t('menu.removeDefaultTooltip'),
            onClick: () => setUserDefault(null),
        });
    } else {
        menuItems.push({
            type: 'item',
            key: 'set-default',
            title: t('menu.makeDefault'),
            tooltip: t('menu.makeDefaultTooltip'),
            onClick: () => setUserDefault(view.urn),
        });
    }

    if (showRemoveGlobalDefaultView) {
        menuItems.push({
            type: 'item',
            key: 'remove-global-default',
            title: t('menu.removeOrgDefault'),
            tooltip: t('menu.removeOrgDefaultTooltip'),
            onClick: () => setGlobalDefault(null),
        });
    }

    if (showSetGlobalDefaultView) {
        menuItems.push({
            type: 'item',
            key: 'set-global-default',
            title: t('menu.makeOrgDefault'),
            tooltip: t('menu.makeOrgDefaultTooltip'),
            onClick: () => setGlobalDefault(view.urn),
        });
    }

    if (canManageView) {
        menuItems.push({
            type: 'item',
            key: 'delete',
            title: tc('delete'),
            tooltip: t('menu.deleteTooltip'),
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
                    {VERTICAL_ELLIPSIS}
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
