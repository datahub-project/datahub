import { Text, Tooltip } from '@components';
import { Property } from '@src/app/tests/builder/steps/definition/builder/property/types/properties';
import { Select } from 'antd';
import React from 'react';
import { StyledSelect } from './styledComponents';

interface Props {
    selectedProperty?: string;
    properties: Property[];
    onChangeProperty: (propertyId) => void;
}

const PropertySelect = ({ selectedProperty, properties, onChangeProperty }: Props) => {
    return (
        <StyledSelect
            value={selectedProperty}
            onChange={onChangeProperty}
            placeholder="Select a property"
            defaultActiveFirstOption={false}
            data-testid="condition-select"
        >
            {properties.map((prop) => (
                <Select.Option key={prop.id} value={prop.id} data-testid={`condition-select-option-${prop.id}`}>
                    <Tooltip title={prop.description} placement="top" showArrow={false}>
                        <Text color="gray" type="span">
                            {prop.displayName}
                        </Text>
                    </Tooltip>
                </Select.Option>
            ))}
        </StyledSelect>
    );
};

export default PropertySelect;
