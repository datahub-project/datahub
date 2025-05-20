import React from 'react';
import styled from 'styled-components/macro';
import { ErModelRelationship } from '@src/types.generated';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';

const CardinalitySidebar = styled.div`
    color: #000000;
    font-weight: 400;
    display: flex;
    flex-flow: column nowrap;
`;

const ERModelRelationshipCardinality = styled.span`
    display: flex;
    align-items: center;
    gap: 10px;
`;

const ERModelRelationshipSidebarCardinality = () => {
    const { entityData } = useEntityData();
    return (
        <CardinalitySidebar>
            <SidebarHeader title="Cardinality" />
            <ERModelRelationshipCardinality>
                <>{(entityData as ErModelRelationship)?.properties?.cardinality}</>
            </ERModelRelationshipCardinality>
        </CardinalitySidebar>
    );
};

export default ERModelRelationshipSidebarCardinality;