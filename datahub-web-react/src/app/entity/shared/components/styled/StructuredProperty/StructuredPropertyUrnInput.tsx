import React, { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import UrnInput from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
import { getInitialEntitiesForUrnPrompt } from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/utils';
import { Maybe, PropertyCardinality, SchemaFieldEntity, StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: any[];
    updateSelectedValues: (values: any[]) => void;
    fieldEntity?: Maybe<SchemaFieldEntity>;
}

export default function StructuredPropertyUrnInput({
    structuredProperty,
    selectedValues,
    updateSelectedValues,
    fieldEntity,
}: Props) {
    const { entityData } = useEntityData();
    const initialEntities = useMemo(
        () => getInitialEntitiesForUrnPrompt(structuredProperty.urn, entityData, selectedValues, fieldEntity),
        [structuredProperty.urn, entityData, selectedValues, fieldEntity],
    );

    const allowedEntityTypes = structuredProperty.definition.typeQualifier?.allowedTypes?.map(
        (allowedType) => allowedType.info.type,
    );
    const isMultiple = structuredProperty.definition.cardinality === PropertyCardinality.Multiple;

    return (
        <UrnInput
            initialEntities={initialEntities}
            allowedEntityTypes={allowedEntityTypes}
            isMultiple={isMultiple}
            selectedValues={selectedValues}
            updateSelectedValues={updateSelectedValues}
        />
    );
}
