import React from 'react';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CopyLinkMenuItem from './items/CopyLinkMenuItem';
import CopyUrnMenuItem from './items/CopyUrnMenuItem';
import EmailMenuItem from './items/EmailMenuItem';
import { StyledMenuItem } from './styledComponents';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

export default function ShareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps) {
    const entityRegistry = useEntityRegistry();
    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

    return (
        <>
            <StyledMenuItem>{navigator.clipboard && <CopyLinkMenuItem key="0" />}</StyledMenuItem>
            <StyledMenuItem>
                {navigator.clipboard && <CopyUrnMenuItem key="1" urn={urn} type={displayType} />}
            </StyledMenuItem>
            <StyledMenuItem>
                <EmailMenuItem key="2" urn={urn} name={displayName} type={displayType} />
            </StyledMenuItem>
        </>
    );
}
