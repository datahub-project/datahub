import React from 'react';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../../types.generated';
import GlossaryEntitiesList from '../../glossary/GlossaryEntitiesList';
import { useEntityRegistry } from '../../useEntityRegistry';
import { sortGlossaryTerms } from '../glossaryTerm/utils';
import { useEntityData } from '../shared/EntityContext';
import { sortGlossaryNodes } from './utils';

function ChildrenTab() {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const childNodes = entityData?.children?.relationships
        .filter((child) => child.entity?.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA.entity, nodeB.entity))
        .map((child) => child.entity);
    const childTerms = entityData?.children?.relationships
        .filter((child) => child.entity?.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA.entity, termB.entity))
        .map((child) => child.entity);

    return (
        <GlossaryEntitiesList
            nodes={(childNodes as GlossaryNode[]) || []}
            terms={(childTerms as GlossaryTerm[]) || []}
        />
    );
}

export default ChildrenTab;
