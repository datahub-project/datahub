import React from 'react';
import styled from 'styled-components';
import { Menu, MenuProps } from 'antd';
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

export default function shareButtonMenu({ urn, entityType, subType, name }: ShareButtonMenuProps): MenuProps {
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
