import React from 'react';
import styled from 'styled-components';

import DateInput from '@app/entity/shared/components/styled/StructuredProperty/DateInput';
import MultiSelectInput from '@app/entity/shared/components/styled/StructuredProperty/MultiSelectInput';
import NumberInput from '@app/entity/shared/components/styled/StructuredProperty/NumberInput';
import RichTextInput from '@app/entity/shared/components/styled/StructuredProperty/RichTextInput';
import SingleSelectInput from '@app/entity/shared/components/styled/StructuredProperty/SingleSelectInput';
import StringInput from '@app/entity/shared/components/styled/StructuredProperty/StringInput';
import StructuredPropertyUrnInput from '@app/entity/shared/components/styled/StructuredProperty/StructuredPropertyUrnInput';
import EntityFormContextProvider from '@app/entity/shared/entityForm/EntityFormContextProvider';

import { PropertyCardinality, StdDataType, StructuredPropertyEntity } from '@types';

const Container = styled.div`
    margin-left: 8px;
`;

interface Props {
    structuredProperty: StructuredPropertyEntity | null;
    selectedValues: string[];
    onChangeValues: (values: string[]) => void;
    isUnsetAction?: boolean;
}

export const StructuredPropertyValueInput = ({
    structuredProperty,
    selectedValues,
    onChangeValues,
    isUnsetAction = false,
}: Props) => {
    if (!structuredProperty || isUnsetAction) {
        return null;
    }

    const { allowedValues, cardinality, valueType } = structuredProperty.definition;

    // Convert string[] to (string | number | null)[] for compatibility with existing components
    const convertedSelectedValues = selectedValues.map((val) => val as string | number | null);

    // Handlers that convert back to string[]
    const selectSingleValue = (value: string | number) => {
        onChangeValues([String(value)]);
    };

    const toggleSelectedValue = (value: string | number) => {
        const stringValue = String(value);
        if (selectedValues.includes(stringValue)) {
            onChangeValues(selectedValues.filter((v) => v !== stringValue));
        } else {
            onChangeValues([...selectedValues, stringValue]);
        }
    };

    const updateSelectedValues = (values: (string | number | null)[]) => {
        onChangeValues(values.map((v) => String(v || '')).filter((v) => v !== ''));
    };

    return (
        <Container>
            <EntityFormContextProvider formUrn="">
                {/* Handle allowed values (dropdown/radio) */}
                {allowedValues && allowedValues.length > 0 && (
                    <>
                        {cardinality === PropertyCardinality.Single && (
                            <SingleSelectInput
                                allowedValues={allowedValues}
                                selectedValues={convertedSelectedValues}
                                selectSingleValue={selectSingleValue}
                            />
                        )}
                        {cardinality === PropertyCardinality.Multiple && (
                            <MultiSelectInput
                                allowedValues={allowedValues}
                                selectedValues={convertedSelectedValues}
                                toggleSelectedValue={toggleSelectedValue}
                                updateSelectedValues={updateSelectedValues}
                            />
                        )}
                    </>
                )}
                {/* Handle different data types */}
                {!allowedValues && valueType?.info?.type === StdDataType.String && (
                    <StringInput
                        selectedValues={convertedSelectedValues}
                        cardinality={cardinality}
                        updateSelectedValues={updateSelectedValues}
                    />
                )}
                {!allowedValues && valueType?.info?.type === StdDataType.RichText && (
                    <RichTextInput
                        selectedValues={convertedSelectedValues}
                        updateSelectedValues={updateSelectedValues}
                    />
                )}
                {!allowedValues && valueType?.info?.type === StdDataType.Date && (
                    <DateInput selectedValues={convertedSelectedValues} updateSelectedValues={updateSelectedValues} />
                )}
                {!allowedValues && valueType?.info?.type === StdDataType.Number && (
                    <NumberInput
                        selectedValues={convertedSelectedValues}
                        cardinality={cardinality}
                        updateSelectedValues={updateSelectedValues}
                    />
                )}
                {!allowedValues && valueType?.info?.type === StdDataType.Urn && (
                    <StructuredPropertyUrnInput
                        structuredProperty={structuredProperty}
                        selectedValues={convertedSelectedValues}
                        updateSelectedValues={updateSelectedValues}
                    />
                )}
            </EntityFormContextProvider>
        </Container>
    );
};
