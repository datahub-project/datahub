import React from 'react';
import { EntityType } from '../../../types.generated';
import { EntitySearchInput } from '../../entityV2/shared/EntitySearchInput/EntitySearchInput';
import { EntitiesSectionProps } from './types';

/**
 * Component for entity selection when creating a tag
 */
const EntitiesSection: React.FC<EntitiesSectionProps> = ({ selectedEntityUrns, setSelectedEntityUrns }) => {
    // Common entity types that can be tagged
    const TAGGABLE_ENTITY_TYPES = [
        EntityType.Dataset,
        EntityType.Dashboard,
        EntityType.Chart,
        EntityType.DataFlow,
        EntityType.DataJob,
        EntityType.GlossaryTerm,
        EntityType.GlossaryNode,
        EntityType.Container,
        EntityType.Mlmodel,
        EntityType.MlfeatureTable,
        EntityType.Mlfeature,
    ];

    return (
        <EntitySearchInput
            selectedUrns={selectedEntityUrns}
            entityTypes={TAGGABLE_ENTITY_TYPES}
            onChangeSelectedUrns={setSelectedEntityUrns}
            placeholder="Search for entities to tag"
            style={{ width: '100%' }}
        />
    );
};

export default EntitiesSection;
