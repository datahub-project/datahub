import React from 'react';
import { PropertyCardinality, StdDataType, StructuredPropertyEntity } from '../../../../../../types.generated';
import SingleSelectInput from './SingleSelectInput';
import MultiSelectInput from './MultiSelectInput';
import StringInput from './StringInput';
import RichTextInput from './RichTextInput';
import DateInput from './DateInput';
import NumberInput from './NumberInput';
import UrnInput from '../../../entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: (string | number | null)[];
    selectSingleValue: (value: string | number) => void;
    toggleSelectedValue: (value: string | number) => void;
    updateSelectedValues: (value: (string | number | null)[]) => void;
}

export default function StructuredPropertyInput({
    structuredProperty,
    selectSingleValue,
    selectedValues,
    toggleSelectedValue,
    updateSelectedValues,
}: Props) {
    const { allowedValues, cardinality, valueType } = structuredProperty.definition;

    return (
        <>
            {allowedValues && allowedValues.length > 0 && (
                <>
                    {cardinality === PropertyCardinality.Single && (
                        <SingleSelectInput
                            allowedValues={allowedValues}
                            selectedValues={selectedValues}
                            selectSingleValue={selectSingleValue}
                        />
                    )}
                    {cardinality === PropertyCardinality.Multiple && (
                        <MultiSelectInput
                            allowedValues={allowedValues}
                            selectedValues={selectedValues}
                            toggleSelectedValue={toggleSelectedValue}
                            updateSelectedValues={updateSelectedValues}
                        />
                    )}
                </>
            )}
            {!allowedValues && valueType.info.type === StdDataType.String && (
                <StringInput
                    selectedValues={selectedValues}
                    cardinality={cardinality}
                    updateSelectedValues={updateSelectedValues}
                />
            )}
            {!allowedValues && valueType.info.type === StdDataType.RichText && (
                <RichTextInput selectedValues={selectedValues} updateSelectedValues={updateSelectedValues} />
            )}
            {!allowedValues && valueType.info.type === StdDataType.Date && (
                <DateInput selectedValues={selectedValues} updateSelectedValues={updateSelectedValues} />
            )}
            {!allowedValues && valueType.info.type === StdDataType.Number && (
                <NumberInput
                    selectedValues={selectedValues}
                    cardinality={cardinality}
                    updateSelectedValues={updateSelectedValues}
                />
            )}
            {!allowedValues && valueType.info.type === StdDataType.Urn && (
                <UrnInput
                    structuredProperty={structuredProperty}
                    selectedValues={selectedValues}
                    updateSelectedValues={updateSelectedValues}
                />
            )}
        </>
    );
}
