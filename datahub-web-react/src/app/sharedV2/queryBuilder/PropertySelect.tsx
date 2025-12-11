/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SelectOption, SimpleSelect } from '@components';
import React, { useMemo } from 'react';

import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ConditionElementWithFixedWidth } from '@app/sharedV2/queryBuilder/styledComponents';

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
        <ConditionElementWithFixedWidth>
            <SimpleSelect
                options={options}
                onUpdate={(val) => onChangeProperty(val[0])}
                values={selectedProperty ? [selectedProperty] : []}
                placeholder="Select a property"
                data-testid="condition-select"
                width="full"
                showClear={false}
            />
        </ConditionElementWithFixedWidth>
    );
};

export default PropertySelect;
