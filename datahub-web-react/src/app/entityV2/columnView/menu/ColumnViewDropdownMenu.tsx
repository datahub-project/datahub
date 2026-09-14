import { useApolloClient } from '@apollo/client';
import { MoreOutlined } from '@ant-design/icons';
import { Dropdown, message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { removeFromColumnViewSelectCaches } from '@app/entityV2/columnView/cacheUtils';
import { useColumnViewContext } from '@app/entityV2/columnView/ColumnViewContext';
import { SCHEMA_TARGET } from '@app/entityV2/columnView/types';
import { useUserContext } from '@app/context/useUserContext';

import {
    useDeleteColumnViewMutation,
    useUpdateCorpUserColumnViewsSettingsMutation,
    useUpdateGlobalColumnViewsSettingsMutation,
} from '@graphql/columnView.generated';
import { DataHubColumnView, DataHubViewType } from '@types';

type Props = {
    view: DataHubColumnView;
    onEdit: () => void;
};

/** Thin parameterized copy of view/menu/ViewDropdownMenu. */
export default function ColumnViewDropdownMenu({ view, onEdit }: Props) {
    const { t } = useTranslation();
    const client = useApolloClient();
    const userContext = useUserContext();
    const { setSelectedUrn } = useColumnViewContext();
    const [deleteView] = useDeleteColumnViewMutation();
    const [setUserDefault] = useUpdateCorpUserColumnViewsSettingsMutation();
    const [setGlobalDefault] = useUpdateGlobalColumnViewsSettingsMutation();

    const isOwner = view.created?.actor === userContext.user?.urn;
    const canManageGlobal = Boolean(userContext.platformPrivileges?.manageGlobalViews);
    const canEdit = view.viewType === DataHubViewType.Global ? canManageGlobal : isOwner;

    const items = [
        canEdit && { key: 'edit', label: t('common.edit'), onClick: onEdit },
        {
            key: 'myDefault',
            label: t('columnViews.makeMyDefault'),
            onClick: () =>
                setUserDefault({ variables: { input: { target: SCHEMA_TARGET, defaultView: view.urn } } })
                    .then(() => userContext.refetchUser?.())
                    .catch((e) => message.error(e.message)),
        },
        canManageGlobal &&
            view.viewType === DataHubViewType.Global && {
                key: 'orgDefault',
                label: t('columnViews.makeOrgDefault'),
                onClick: () =>
                    setGlobalDefault({
                        variables: { input: { target: SCHEMA_TARGET, defaultView: view.urn } },
                        refetchQueries: ['getGlobalColumnViewsSettings'],
                    }).catch((e) => message.error(e.message)),
            },
        canEdit && {
            key: 'delete',
            danger: true,
            label: t('common.delete'),
            onClick: () =>
                deleteView({ variables: { urn: view.urn } })
                    .then(() => {
                        removeFromColumnViewSelectCaches(view.urn, client);
                        setSelectedUrn(undefined);
                    })
                    .catch((e) => message.error(e.message)),
        },
    ].filter(Boolean) as any[];

    return (
        <Dropdown menu={{ items }} trigger={['click']}>
            <MoreOutlined onClick={(e) => e.stopPropagation()} />
        </Dropdown>
    );
}
