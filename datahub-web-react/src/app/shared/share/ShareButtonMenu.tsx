import React from 'react';
import styled from 'styled-components';
import { Menu } from 'antd';
import { EntityType } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import CopyLinkMenuItem from './items/CopyLinkMenuItem';
import CopyUrnMenuItem from './items/CopyUrnMenuItem';
import EmailMenuItem from './items/EmailMenuItem';
import { useEntityRegistry } from '../../useEntityRegistry';

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
`;

export default function ShareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps) {
    const entityRegistry = useEntityRegistry();
    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

    return (
        <StyledMenu selectable={false}>
            {navigator.clipboard && <CopyLinkMenuItem key="0" />}
            {navigator.clipboard && <CopyUrnMenuItem key="1" urn={urn} type={displayType} />}
            <EmailMenuItem key="2" urn={urn} name={displayName} type={displayType} />
        </StyledMenu>
    );
}
