import { Icon } from '@src/alchemy-components';
import { Property } from '@src/app/tests/builder/steps/definition/builder/property/types/properties';
import { getOperatorOptions, getValueOptions } from '@src/app/tests/builder/steps/definition/builder/property/utils';
import { PropertyPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import React from 'react';
import { CardIcons } from '../styledComponents';
import OperatorSelect from './OperatorSelect';
import PropertySelect from './PropertySelect';
import { ConditionContainer, IconsContainer, SelectContainer } from './styledComponents';
import ValuesSelect from './ValuesSelect';

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
        const newPredicate = {
            property: propertyId,
        };
        onChangePredicate(newPredicate);
    };

    const handleOperatorChange = (operatorId: string) => {
        const newPredicate = {
            ...selectedPredicate,
            operator: operatorId,
            values: undefined,
        };
        onChangePredicate(newPredicate);
    };

    const handleValuesChange = (values: string[]) => {
        const newPredicate = {
            ...selectedPredicate,
            values,
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
