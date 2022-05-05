import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '../../../EntityContext';
import { SidebarHeader } from './SidebarHeader';
import { CompactEntityNameList } from '../../../../../recommendations/renderer/component/CompactEntityNameList';
import { Entity } from '../../../../../../types.generated';

const EntityListContainer = styled.div`
    margin-left: -8px;
`;

export const SidebarSiblingsSection = () => {
    const { entityData } = useEntityData();

    const siblings = entityData?.siblings;

    return (
        <div>
            <SidebarHeader title={siblings?.isPrimary ? 'Backed by' : 'Connected to'} />
            <EntityListContainer>
                <CompactEntityNameList entities={(siblings?.siblings as Entity[]) || []} />
            </EntityListContainer>
        </div>
    );
};
