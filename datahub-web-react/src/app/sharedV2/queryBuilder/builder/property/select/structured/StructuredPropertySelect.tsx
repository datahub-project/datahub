import { Select } from 'antd';
import React, { useEffect } from 'react';

import { StructuredPropertySelectResult } from '@app/sharedV2/queryBuilder/builder/property/select/structured/StructuredPropertySelectResult';
import { createPropertyUrnMap } from '@app/sharedV2/queryBuilder/builder/property/select/structured/utils';

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
    console.log(entityTypes);

    const [searchProperties, { data: propertiesSearchData }] = useSearchStructuredPropertiesLazyQuery();
    const searchResults = propertiesSearchData?.searchAcrossEntities?.searchResults || [];
    const properties = searchResults.map((result) => result.entity) as StructuredPropertyEntity[];

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
