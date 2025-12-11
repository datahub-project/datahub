/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import { SearchSelectUrnInput } from '@src/app/entityV2/shared/components/styled/search/SearchSelectUrnInput';

import { EntityType, PropertyCardinality, StructuredPropertyEntity } from '@types';

interface StructuredPropertySearchSelectUrnInputProps {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: string[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

// Wrapper component that extracts information from StructuredProperty
export default function StructuredPropertySearchSelectUrnInput({
    structuredProperty,
    selectedValues,
    updateSelectedValues,
}: StructuredPropertySearchSelectUrnInputProps) {
    // Get the allowed entity types from the structured property
    const allowedEntityTypes = useMemo(() => {
        return (
            structuredProperty.definition.typeQualifier?.allowedTypes?.map(
                (allowedType) => allowedType.info.type as EntityType,
            ) || []
        );
    }, [structuredProperty]);

    const isMultiple = structuredProperty.definition.cardinality === PropertyCardinality.Multiple;

    return (
        <SearchSelectUrnInput
            allowedEntityTypes={allowedEntityTypes}
            isMultiple={isMultiple}
            selectedValues={selectedValues}
            updateSelectedValues={updateSelectedValues}
        />
    );
}
