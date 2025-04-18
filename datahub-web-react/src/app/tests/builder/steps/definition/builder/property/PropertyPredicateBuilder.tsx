import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { CustomPropertyPredicateBuilder } from '@app/tests/builder/steps/definition/builder/property/CustomPropertyPredicateBuilder';
import { OwnershipTypePredicateBuilder } from '@app/tests/builder/steps/definition/builder/property/OwnershipTypePredicateBuilder';
import { StructuredPropertyPredicateBuilder } from '@app/tests/builder/steps/definition/builder/property/StructuredPropertyPredicateBuilder';
import { TypedPropertyPredicateBuilder } from '@app/tests/builder/steps/definition/builder/property/TypedPropertyPredicateBuilder';
import { Property } from '@app/tests/builder/steps/definition/builder/property/types/properties';
import {
    getPropertyById,
    isOwnershipTypeId,
    isStructuredPropertyId,
} from '@app/tests/builder/steps/definition/builder/property/utils';
import { PropertyPredicate } from '@app/tests/builder/steps/definition/builder/types';

const PredicateContainer = styled.div`
    display: flex;
    align-items: center;
`;

type Props = {
    selectedPredicate?: PropertyPredicate;
    properties: Property[];
    onChangePredicate: (newPredicate: PropertyPredicate) => void;
};

/**
 * Which builder to use for editing the selected predicate
 */
enum PropertyPredicateBuilderType {
    /**
     * Display the default property select.
     */
    PROPERTY_SELECT = 'PROPERTY_SELECT',
    /**
     * Display the structured property predicate builder. Used for generating predicates that reference specific structured properties
     */
    STRUCTURED_PROPERTY = 'STRUCTURED_PROPERTY',
    /**
     * Display the ownership type property predicate builder. Used for generating predicates that reference specific custom ownership types
     */
    OWNERSHIP_TYPE = 'OWNERSHIP_TYPE',
    /**
     * Display the freeform custom property builder. Used when a property cannot be found (is not supported) by the default property select.
     */
    CUSTOM = 'CUSTOM',
}

/**
 * This component allows you to construct a single Property Predicate.
 */
export const PropertyPredicateBuilder = ({ selectedPredicate, properties, onChangePredicate }: Props) => {
    /**
     * Controls which experience is displayed for building a predicate.
     *
     * By default, we show the experience allowing using to select a property from a list
     * then an operator then a set of values.
     */
    const [builderType, setBuilderType] = useState(PropertyPredicateBuilderType.PROPERTY_SELECT);

    /**
     * Apply the following logic to determine which builder to display:
     *
     * 0. If no property has been selected yet (it's undefined), we show the default Property Select as
     *    the entry point for property selection.
     *
     * 1. If the property id matches the shape of a "structured property reference", e.g. "structuredProperties.urn:li:structuredProperty:xyz",
     *    then display the structured property predicate builder.
     *
     * 2. If the property id matches the shape of a "custom ownership type reference", e.g. "ownership.ownerTypes.urn:li:ownershipType:xyz",
     *    then display the ownership type predicate builder.
     *
     * 3. If the property id can be used to find a property object definition from the provided "properties" list above,
     *    then the property is well-supported by the default property select experience, so display that.
     *
     * 4. If the property is unrecognized, then fall back to showing the "custom property editor", which is really just a
     *    freeform text box where you can enter the name of a specific property instead of selecting it.
     */
    useEffect(() => {
        const selectedPropertyId = selectedPredicate?.property;

        if (!selectedPropertyId) {
            // Case 0
            setBuilderType(PropertyPredicateBuilderType.PROPERTY_SELECT);
        } else if (isStructuredPropertyId(selectedPropertyId)) {
            // Case 1
            setBuilderType(PropertyPredicateBuilderType.STRUCTURED_PROPERTY);
        } else if (isOwnershipTypeId(selectedPropertyId)) {
            // Case 2
            setBuilderType(PropertyPredicateBuilderType.OWNERSHIP_TYPE);
        } else if (getPropertyById(selectedPropertyId, properties)) {
            // Case 3
            setBuilderType(PropertyPredicateBuilderType.PROPERTY_SELECT);
        } else {
            // Case 4
            setBuilderType(PropertyPredicateBuilderType.CUSTOM);
        }
    }, [selectedPredicate, properties, setBuilderType]);

    const onChangeProperty = (propertyId?: string) => {
        const newPredicate = {
            property: propertyId,
        };
        onChangePredicate(newPredicate);
    };

    const onChangeOperator = (operatorId?: string) => {
        const newPredicate = {
            ...selectedPredicate,
            operator: operatorId,
            values: undefined,
        };
        onChangePredicate(newPredicate);
    };

    const onChangeValues = (values?: string[]) => {
        const newPredicate = {
            ...selectedPredicate,
            values,
        };
        onChangePredicate(newPredicate);
    };

    return (
        <PredicateContainer>
            {(builderType === PropertyPredicateBuilderType.PROPERTY_SELECT && (
                <TypedPropertyPredicateBuilder
                    selectedPredicate={selectedPredicate}
                    properties={properties}
                    onChangeProperty={onChangeProperty}
                    onChangeOperator={onChangeOperator}
                    onChangeValues={onChangeValues}
                />
            )) ||
                null}
            {builderType === PropertyPredicateBuilderType.STRUCTURED_PROPERTY && (
                <StructuredPropertyPredicateBuilder
                    selectedPredicate={selectedPredicate}
                    onChangeProperty={onChangeProperty}
                    onChangeOperator={onChangeOperator}
                    onChangeValues={onChangeValues}
                />
            )}
            {builderType === PropertyPredicateBuilderType.OWNERSHIP_TYPE && (
                <OwnershipTypePredicateBuilder
                    selectedPredicate={selectedPredicate}
                    onChangeProperty={onChangeProperty}
                    onChangeOperator={onChangeOperator}
                    onChangeValues={onChangeValues}
                />
            )}
            {builderType === PropertyPredicateBuilderType.CUSTOM && (
                <CustomPropertyPredicateBuilder
                    selectedPredicate={selectedPredicate}
                    onChangeProperty={onChangeProperty}
                    onChangeOperator={onChangeOperator}
                    onChangeValues={onChangeValues}
                />
            )}
        </PredicateContainer>
    );
};
