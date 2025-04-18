import React from 'react';
import { MenuProps } from 'antd';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useAppConfig } from '@src/app/useAppConfig';
import { EntityType } from '../../../types.generated';
import CopyLinkMenuItem from './items/CopyLinkMenuItem';
import CopyUrnMenuItem from './items/CopyUrnMenuItem';
import EmailMenuItem from './items/EmailMenuItem';
import { useEntityRegistry } from '../../useEntityRegistry';
import MetadataShareItem from './items/MetadataShareItem/MetadataShareItem';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

export default function useShareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps): MenuProps {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const appConfig = useAppConfig();
    const { metadataShareEnabled } = appConfig.config.featureFlags;
    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;
    const canShareEntity = entityData?.privileges?.canShareEntity;

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

    if (metadataShareEnabled && canShareEntity) {
        items.push({
            key: 3,
            label: <MetadataShareItem key="3" />,
        });
    }

    return { items };
}
