import React, { useMemo } from 'react';
import { PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';
import UrnInput from '../../../entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
import { useEntityData } from '../../../EntityContext';
import { getInitialEntitiesForUrnPrompt } from '../../../entityForm/prompts/StructuredPropertyPrompt/utils';

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: any[];
    updateSelectedValues: (values: any[]) => void;
}

export default function StructuredPropertyUrnInput({
    structuredProperty,
    selectedValues,
    updateSelectedValues,
}: Props) {
    const { entityData } = useEntityData();
    const initialEntities = useMemo(
        () => getInitialEntitiesForUrnPrompt(structuredProperty.urn, entityData, selectedValues),
        [structuredProperty.urn, entityData, selectedValues],
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
