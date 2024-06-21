import React from 'react';
import styled from 'styled-components';
import { Menu } from 'antd';
import { EntityType } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import CopyLinkMenuItem from './items/CopyLinkMenuItem';
import CopyUrnMenuItem from './items/CopyUrnMenuItem';
import EmailMenuItem from './items/EmailMenuItem';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useAppConfig } from '../../useAppConfig';
import MetadataShareItem from './items/MetadataShareItem/MetadataShareItem';
import { useEntityData } from '../../entity/shared/EntityContext';

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
