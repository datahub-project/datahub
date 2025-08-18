import { Select } from 'antd';
import React, { useEffect } from 'react';

import { StructuredPropertySelectResult } from '@app/tests/builder/steps/definition/builder/property/select/structured/StructuredPropertySelectResult';
import { createPropertyUrnMap } from '@app/tests/builder/steps/definition/builder/property/select/structured/utils';

import { useSearchStructuredPropertiesLazyQuery } from '@graphql/structuredProperties.generated';
import { EntityType, StructuredPropertyEntity } from '@types';

type Props = {
    selectedProperty?: StructuredPropertyEntity; // The selected property
    entityTypes?: EntityType[]; // The set of entity types for the structured property.
    onSelect: (property: StructuredPropertyEntity) => void;
    onClear: () => void;
};

/**
 * Select a structured property from a dropdown. Allow search values.
 */
export const StructuredPropertySelect = ({ selectedProperty, entityTypes, onSelect, onClear }: Props) => {
    const [searchProperties, { data: propertiesSearchData }] = useSearchStructuredPropertiesLazyQuery();
    const searchResults = propertiesSearchData?.searchAcrossEntities?.searchResults || [];
    const allProperties = searchResults.map((result) => result.entity) as StructuredPropertyEntity[];

    // Filter structured properties to only show those that are compatible with the selected entity types
    const properties = allProperties.filter((property) => {
        if (!entityTypes || entityTypes.length === 0) {
            // If no entity types are selected, show all properties
            return true;
        }

        const propertyEntityTypes = property.definition?.entityTypes?.map((et) => et.info?.type) || [];
        if (propertyEntityTypes.length === 0) {
            // If the structured property doesn't specify entity types, it applies to all entities
            return true;
        }

        // Check if at least one of the selected entity types is supported by this structured property
        return entityTypes.some((selectedType) => propertyEntityTypes.includes(selectedType));
    });

    // Local of property urn to property object.
    const urnToProperty = createPropertyUrnMap(properties);

    const onSearch = (text: string) => {
        searchProperties({
            variables: {
                query: text,
                start: 0,
                count: 10,
            },
        });
    };

    useEffect(() => {
        searchProperties({
            variables: {
                query: '*',
                start: 0,
                count: 20,
            },
        });
    }, [searchProperties]);

    return (
        <Select
            autoFocus
            allowClear
            autoClearSearchValue={false}
            showSearch={!selectedProperty?.urn}
            maxTagCount={1}
            style={{ width: 240, marginRight: 8 }}
            dropdownMatchSelectWidth={400}
            value={(selectedProperty?.urn && [selectedProperty?.urn]) || []}
            filterOption={false}
            placeholder="Search for structured properties..."
            onSelect={(propertyUrn) => {
                onSelect(urnToProperty.get(propertyUrn));
            }}
            onSearch={onSearch}
            onClear={onClear}
            showArrow={false}
        >
            {properties?.map((property) => (
                <Select.Option value={property.urn}>
                    <StructuredPropertySelectResult property={property} />
                </Select.Option>
            ))}
        </Select>
    );
};
