import React from 'react';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../../types.generated';
import GlossaryEntitiesList from '../../glossary/GlossaryEntitiesList';
import { useEntityData } from '../shared/EntityContext';

function ChildrenTab() {
    const { entityData } = useEntityData();

    const childNodes = entityData?.children?.relationships
        .filter((child) => child.entity?.type === EntityType.GlossaryNode)
        .map((child) => child.entity);
    const childTerms = entityData?.children?.relationships
        .filter((child) => child.entity?.type === EntityType.GlossaryTerm)
        .map((child) => child.entity);

    return (
        <GlossaryEntitiesList
            nodes={(childNodes as GlossaryNode[]) || []}
            terms={(childTerms as GlossaryTerm[]) || []}
        />
    );
}

export default ChildrenTab;
