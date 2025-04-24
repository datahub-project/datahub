import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import CopyLinkMenuItem from '@app/shared/share/v2/items/CopyLinkMenuItem';
import CopyUrnMenuItem from '@app/shared/share/v2/items/CopyUrnMenuItem';
import EmailMenuItem from '@app/shared/share/v2/items/EmailMenuItem';
import MetadataShareItem from '@app/shared/share/v2/items/MetadataShareItem/MetadataShareItem';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

export default function ShareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps) {
    const entityRegistry = useEntityRegistry();
    const appConfig = useAppConfig();
    const { entityData } = useEntityData();

    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;
    const { metadataShareEnabled } = appConfig.config.featureFlags;

    // User based permissions
    const canShareEntity = entityData?.privileges?.canShareEntity;

    return (
        <>
            {navigator.clipboard && <CopyLinkMenuItem key="0" urn={urn} entityType={entityType} />}

            {navigator.clipboard && <CopyUrnMenuItem key="1" urn={urn} type={displayType} />}

            {metadataShareEnabled && canShareEntity && <MetadataShareItem key="2" />}

            <EmailMenuItem key="2" urn={urn} name={displayName} type={displayType} />
        </>
    );
}
