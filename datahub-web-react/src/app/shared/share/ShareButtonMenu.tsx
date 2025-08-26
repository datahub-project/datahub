import { Menu } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import CopyLinkMenuItem from '@app/shared/share/items/CopyLinkMenuItem';
import CopyUrnMenuItem from '@app/shared/share/items/CopyUrnMenuItem';
import EmailMenuItem from '@app/shared/share/items/EmailMenuItem';
import MetadataShareItem from '@app/shared/share/items/MetadataShareItem/MetadataShareItem';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

interface ShareButtonMenuProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

const StyledMenu = styled(Menu)`
    border: 1px solid ${ANTD_GRAY[3]};
    border-radius: 4px;
    min-width: 140px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
`;

export default function ShareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps) {
    const entityRegistry = useEntityRegistry();
    const appConfig = useAppConfig();
    const { entityData } = useEntityData();
    const { metadataShareEnabled } = appConfig.config.featureFlags;
    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

    // User based permissions
    const canShareEntity = entityData?.privileges?.canShareEntity;

    return (
        <StyledMenu selectable={false}>
            {navigator.clipboard && <CopyLinkMenuItem key="0" />}
            {navigator.clipboard && <CopyUrnMenuItem key="1" urn={urn} type={displayType} />}
            {metadataShareEnabled && canShareEntity && <MetadataShareItem key="2" />}
            <EmailMenuItem key="2" urn={urn} name={displayName} type={displayType} />
        </StyledMenu>
    );
}
