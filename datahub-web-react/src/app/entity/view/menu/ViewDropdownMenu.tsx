import React, { useState } from 'react';
import styled from 'styled-components';
import { useApolloClient } from '@apollo/client';
import { MoreOutlined } from '@ant-design/icons';
import { Dropdown, message, Modal } from 'antd';
import { DataHubView, DataHubViewType } from '../../../../types.generated';
import { useUserContext } from '../../../context/useUserContext';
import { useUpdateCorpUserViewsSettingsMutation } from '../../../../graphql/user.generated';
import { useUpdateGlobalViewsSettingsMutation } from '../../../../graphql/app.generated';
import { useDeleteViewMutation } from '../../../../graphql/view.generated';
import { removeFromListMyViewsCache, removeFromViewSelectCaches } from '../cacheUtils';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../utils';
import { ViewBuilderMode } from '../builder/types';
import { ViewBuilder } from '../builder/ViewBuilder';
import { EditViewItem } from './item/EditViewItem';
import { PreviewViewItem } from './item/PreviewViewItem';
import { RemoveUserDefaultItem } from './item/RemoveUserDefaultItem';
import { SetUserDefaultItem } from './item/SetUserDefaultItem';
import { RemoveGlobalDefaultItem } from './item/RemoveGlobalDefaultItem';
import { SetGlobalDefaultItem } from './item/SetGlobalDefaultItem';
import { DeleteViewItem } from './item/DeleteViewItem';
import analytics, { EventType } from '../../../analytics';

const MenuButton = styled(MoreOutlined)`
    width: 20px;
    &&& {
        padding-left: 0px;
        padding-right: 0px;
        font-size: 18px;
    }
    :hover {
        cursor: pointer;
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
};

export const ViewDropdownMenu = ({
    view,
    visible,
    isOwnedByUser = view.viewType === DataHubViewType.Personal,
    trigger = 'hover',
    onClickEdit,
    onClickPreview,
    onClickDelete,
}: Props) => {
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

    const items = [
        {
            key: 0,
            label: (canManageView && <EditViewItem key="0" onClick={onEditView} />) || (
                <PreviewViewItem key="0" onClick={onPreviewView} />
            ),
        },
        {
            key: 1,
            label: (isUserDefault && <RemoveUserDefaultItem key="1" onClick={() => setUserDefault(null)} />) || (
                <SetUserDefaultItem key="1" onClick={() => setUserDefault(view.urn)} />
            ),
        },
        showRemoveGlobalDefaultView
            ? {
                  key: 2,
                  label: <RemoveGlobalDefaultItem key="2" onClick={() => setGlobalDefault(null)} />,
              }
            : null,
        showSetGlobalDefaultView
            ? {
                  key: 2,
                  label: <SetGlobalDefaultItem key="2" onClick={() => setGlobalDefault(view.urn)} />,
              }
            : null,
        canManageView
            ? {
                  key: 3,
                  label: <DeleteViewItem key="3" onClick={confirmDeleteView} />,
              }
            : null,
    ];

    return (
        <>
            <Dropdown menu={{ items }} trigger={[trigger]}>
                <MenuButton data-testid="views-table-dropdown" style={{ display: visible ? undefined : 'none' }} />
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
