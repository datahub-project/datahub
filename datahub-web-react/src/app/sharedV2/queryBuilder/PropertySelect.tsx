import { SelectOption, SimpleSelect } from '@components';
import React, { useMemo } from 'react';

import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';

interface Props {
    selectedProperty?: string;
    properties: Property[];
    onChangeProperty: (propertyId) => void;
}

const PropertySelect = ({ selectedProperty, properties, onChangeProperty }: Props) => {
    const options: SelectOption[] = useMemo(
        () =>
            properties?.map((property) => ({
                value: property.id.toString(),
                label: property.displayName,
                description: property.description,
            })) ?? [],
        [properties],
    );

    return (
        <SimpleSelect
            options={options}
            onUpdate={(val) => onChangeProperty(val[0])}
            values={selectedProperty ? [selectedProperty] : []}
            placeholder="Select a property"
            data-testid="condition-select"
            width="full"
            showClear={false}
        />
    );
};

export default PropertySelect;
