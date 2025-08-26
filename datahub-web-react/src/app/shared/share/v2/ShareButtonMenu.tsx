import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import CopyLinkMenuItem from '@app/shared/share/v2/items/CopyLinkMenuItem';
import CopyNameMenuItem from '@app/shared/share/v2/items/CopyNameMenuItem';
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
    qualifiedName?: string | null;
}

export default function ShareButtonMenu({ urn, entityType, subType, name, qualifiedName }: ShareButtonMenuProps) {
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

            {navigator.clipboard && (
                <CopyNameMenuItem
                    key="2"
                    name={displayName}
                    type={displayType}
                    qualifiedName={qualifiedName || undefined}
                />
            )}

            {metadataShareEnabled && canShareEntity && <MetadataShareItem key="3" />}

            <EmailMenuItem key="4" urn={urn} name={displayName} type={displayType} />
        </>
    );
}
