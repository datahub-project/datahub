import styled from 'styled-components/macro';
import { ErModelRelationship } from '../../../../../types.generated';
import { useEntityData } from '../../../shared/EntityContext';
import { SidebarHeader } from '../../../shared/containers/profile/sidebar/SidebarHeader';

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