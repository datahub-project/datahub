import React from 'react';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CopyLinkMenuItem from './items/CopyLinkMenuItem';
import CopyUrnMenuItem from './items/CopyUrnMenuItem';
import EmailMenuItem from './items/EmailMenuItem';
import CopyNameMenuItem from './items/CopyNameMenuItem';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
    qualifiedName?: string | null;
}

export default function ShareButtonMenu({ urn, entityType, subType, name, qualifiedName }: ShareButtonMenuProps) {
    const entityRegistry = useEntityRegistry();

    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

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

            <EmailMenuItem key="4" urn={urn} name={displayName} type={displayType} />
        </>
    );
}
