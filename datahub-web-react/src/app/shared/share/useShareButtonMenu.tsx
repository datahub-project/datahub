import { MenuProps } from 'antd';
import React from 'react';

import CopyLinkMenuItem from '@app/shared/share/items/CopyLinkMenuItem';
import CopyUrnMenuItem from '@app/shared/share/items/CopyUrnMenuItem';
import EmailMenuItem from '@app/shared/share/items/EmailMenuItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

export default function useShareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps): MenuProps {
    const entityRegistry = useEntityRegistry();
    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

    const items = [
        navigator.clipboard && {
            key: 0,
            label: <CopyLinkMenuItem key="0" />,
        },
        navigator.clipboard && {
            key: 1,
            label: <CopyUrnMenuItem key="1" urn={urn} type={displayType} />,
        },
        {
            key: 2,
            label: <EmailMenuItem key="2" urn={urn} name={displayName} type={displayType} />,
        },
    ];

    return { items };
}
