import { SelectOption, SimpleSelect } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ConditionElementWithFixedWidth } from '@app/sharedV2/queryBuilder/styledComponents';

interface Props {
    selectedProperty?: string;
    properties: Property[];
    onChangeProperty: (propertyId) => void;
}

const PropertySelect = ({ selectedProperty, properties, onChangeProperty }: Props) => {
    const { t } = useTranslation('shared.query-builder');
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
                placeholder={t('property.placeholder')}
                dataTestId="condition-select"
                width="full"
                showClear={false}
            />
        </ConditionElementWithFixedWidth>
    );
};

export default PropertySelect;
