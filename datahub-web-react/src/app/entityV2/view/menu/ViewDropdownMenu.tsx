import { useApolloClient } from '@apollo/client';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import { Dropdown, Menu, Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { removeFromListMyViewsCache, removeFromViewSelectCaches } from '@app/entityV2/view/cacheUtils';
import { DeleteViewItem } from '@app/entityV2/view/menu/item/DeleteViewItem';
import { EditViewItem } from '@app/entityV2/view/menu/item/EditViewItem';
import { PreviewViewItem } from '@app/entityV2/view/menu/item/PreviewViewItem';
import { RemoveGlobalDefaultItem } from '@app/entityV2/view/menu/item/RemoveGlobalDefaultItem';
import { RemoveUserDefaultItem } from '@app/entityV2/view/menu/item/RemoveUserDefaultItem';
import { SetGlobalDefaultItem } from '@app/entityV2/view/menu/item/SetGlobalDefaultItem';
import { SetUserDefaultItem } from '@app/entityV2/view/menu/item/SetUserDefaultItem';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useUpdateGlobalViewsSettingsMutation } from '@graphql/app.generated';
import { useUpdateCorpUserViewsSettingsMutation } from '@graphql/user.generated';
import { useDeleteViewMutation } from '@graphql/view.generated';
import { DataHubView, DataHubViewType } from '@types';

const MenuButton = styled(MoreVertIcon)<{ $isShowNavBarRedesign?: boolean }>`
    width: 20px;
    ${(props) => props.$isShowNavBarRedesign && `color: ${colors.gray[1800]};`}
    &&& {
        padding-left: 0px;
        padding-right: 0px;
        font-size: 18px;
    }
    :hover {
        cursor: pointer;
    }
`;

const MenuStyled = styled(Menu)`
    border-radius: 12px;
    padding: 10px 0px;
    &&& {
        .ant-dropdown-menu-item:not(:hover) {
            background: none;
        }
        .ant-dropdown-menu-item:hover {
            background: #f5f5f5;
        }
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
    // Custom Action Handlers - useful if you do NOT want the Menu to handle Modal rendering.
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

    /**
     * Updates the User's Personal Default View via mutation.
     *
     * Then updates the User Context state to contain the new default.
     */
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
            .catch((_) => {
                message.destroy();
                message.error({
                    content: `Failed to make this your default view. An unexpected error occurred.`,
                    duration: 3,
                });
            });
    };

    /**
     * Updates the Global Default View via mutation.
     *
     * Then updates the User Context state to contain the new default.
     */
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
            .catch((_) => {
                message.destroy();
                message.error({
                    content: `Failed to make this your organization's default view. An unexpected error occurred.`,
                    duration: 3,
                });
            });
    };

    const onEditView = () => {
        if (onClickEdit) {
            onClickEdit?.();
        } else {
            setViewBuilderState({
                mode: ViewBuilderMode.EDITOR,
                visible: true,
            });
        }
    };

    const onPreviewView = () => {
        if (onClickPreview) {
            onClickPreview?.();
        } else {
            setViewBuilderState({
                mode: ViewBuilderMode.PREVIEW,
                visible: true,
            });
        }
    };

    const onViewBuilderClose = () => {
        setViewBuilderState(DEFAULT_VIEW_BUILDER_STATE);
    };

    const deleteView = (viewUrn: string) => {
        deleteViewMutation({
            variables: { urn: viewUrn },
        })
            .then(({ errors }) => {
                if (!errors) {
                    removeFromViewSelectCaches(viewUrn, client);
                    removeFromListMyViewsCache(viewUrn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined, undefined);
                    /**
                     * Clear the selected view urn from local state,
                     * if the deleted view was that urn.
                     */
                    if (viewUrn === userContext.localState?.selectedViewUrn) {
                        userContext.updateLocalState({
                            ...userContext.localState,
                            selectedViewUrn: undefined,
                        });
                    }
                    message.success({ content: 'Removed View!', duration: 2 });
                }
            })
            .catch(() => {
                message.destroy();
                message.error({
                    content: `Failed to delete View. An unexpected error occurred.`,
                    duration: 3,
                });
            });
    };

    const confirmDeleteView = () => {
        if (onClickDelete) {
            onClickDelete?.();
        } else {
            Modal.confirm({
                title: `Confirm Remove ${view.name}`,
                content: `Are you sure you want to remove this View?`,
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

    const handleDropdownClick = (e) => {
        e.stopPropagation();
    };

    return (
        <>
            <Dropdown
                overlay={
                    <MenuStyled>
                        {(canManageView && <EditViewItem key="0" onClick={onEditView} />) || (
                            <PreviewViewItem key="0" onClick={onPreviewView} />
                        )}
                        {(isUserDefault && <RemoveUserDefaultItem key="1" onClick={() => setUserDefault(null)} />) || (
                            <SetUserDefaultItem key="1" onClick={() => setUserDefault(view.urn)} />
                        )}
                        {showRemoveGlobalDefaultView && (
                            <RemoveGlobalDefaultItem key="2" onClick={() => setGlobalDefault(null)} />
                        )}
                        {showSetGlobalDefaultView && (
                            <SetGlobalDefaultItem key="2" onClick={() => setGlobalDefault(view.urn)} />
                        )}
                        {canManageView && <DeleteViewItem key="3" onClick={confirmDeleteView} />}
                    </MenuStyled>
                }
                trigger={[trigger]}
            >
                <MenuButton
                    data-testid="views-table-dropdown"
                    style={{ display: visible ? undefined : 'none' }}
                    onClick={handleDropdownClick}
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                />
            </Dropdown>
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
