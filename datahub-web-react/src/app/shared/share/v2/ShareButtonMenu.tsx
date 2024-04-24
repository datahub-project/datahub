import React from 'react';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CopyLinkMenuItem from './items/CopyLinkMenuItem';
import CopyUrnMenuItem from './items/CopyUrnMenuItem';
import EmailMenuItem from './items/EmailMenuItem';
import { useAppConfig } from '../../../useAppConfig';
import { useEntityData } from '../../../entity/shared/EntityContext';
import MetadataShareItem from './items/MetadataShareItem/MetadataShareItem';

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
