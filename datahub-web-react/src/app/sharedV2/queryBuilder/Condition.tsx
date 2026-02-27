import React from 'react';

import OperatorSelect from '@app/sharedV2/queryBuilder/OperatorSelect';
import PropertySelect from '@app/sharedV2/queryBuilder/PropertySelect';
import ValuesSelect from '@app/sharedV2/queryBuilder/ValuesSelect';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { getOperatorOptions, getValueOptions } from '@app/sharedV2/queryBuilder/builder/property/utils';
import { PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import {
    CardIcons,
    ConditionContainer,
    IconsContainer,
    SelectContainer,
} from '@app/sharedV2/queryBuilder/styledComponents';
import { Icon } from '@src/alchemy-components';

interface Props {
    selectedPredicate?: PropertyPredicate;
    onDeletePredicate: (index: number) => void;
    onChangePredicate: (newPredicate: PropertyPredicate) => void;
    properties: Property[];
    index: number;
    depth: number;
}

/**
 * Component representing a single condition filter
 */

const Condition = ({ selectedPredicate, onDeletePredicate, onChangePredicate, properties, index, depth }: Props) => {
    const property =
        (selectedPredicate &&
            selectedPredicate.property &&
            properties.find((prop) => prop.id === selectedPredicate.property)) ||
        undefined;

    const operatorOptions = (property?.valueType && getOperatorOptions(property.valueType)) || undefined;
    const valueOptions = (property && selectedPredicate && getValueOptions(property, selectedPredicate)) || undefined;

    const handlePropertyChange = (propertyId?: string) => {
        const newPredicate: PropertyPredicate = {
            type: 'property',
            property: propertyId,
        };
        onChangePredicate(newPredicate);
    };

    const handleOperatorChange = (operatorId: string) => {
        const newPredicate: PropertyPredicate = {
            ...selectedPredicate,
            operator: operatorId,
            values: undefined,
            type: 'property',
        };
        onChangePredicate(newPredicate);
    };

    const handleValuesChange = (values: string[]) => {
        const newPredicate: PropertyPredicate = {
            ...selectedPredicate,
            values,
            type: 'property',
        };
        onChangePredicate(newPredicate);
    };

    return (
        <ConditionContainer depth={depth}>
            <SelectContainer>
                <PropertySelect
                    selectedProperty={selectedPredicate?.property}
                    properties={properties}
                    onChangeProperty={handlePropertyChange}
                />

                <OperatorSelect
                    selectedOperator={selectedPredicate?.operator}
                    operators={operatorOptions}
                    onChangeOperator={handleOperatorChange}
                />

                <ValuesSelect
                    selectedValues={selectedPredicate?.values}
                    options={valueOptions}
                    onChangeValues={handleValuesChange}
                    property={selectedPredicate?.property}
                    propertyDisplayName={property?.displayName}
                />
            </SelectContainer>
            <IconsContainer>
                <CardIcons>
                    <Icon icon="Delete" size="md" onClick={() => onDeletePredicate(index)} />
                </CardIcons>
            </IconsContainer>
        </ConditionContainer>
    );
};

export default Condition;
