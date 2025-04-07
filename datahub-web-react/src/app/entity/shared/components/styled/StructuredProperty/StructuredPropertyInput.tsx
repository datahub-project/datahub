<<<<<<< HEAD
import React from 'react';
import {
    Maybe,
    PropertyCardinality,
    SchemaFieldEntity,
    StdDataType,
    StructuredPropertyEntity,
} from '@src/types.generated';
import SingleSelectInput from './SingleSelectInput';
import MultiSelectInput from './MultiSelectInput';
import StringInput from './StringInput';
import RichTextInput from './RichTextInput';
import DateInput from './DateInput';
import NumberInput from './NumberInput';
import StructuredPropertyUrnInput from './StructuredPropertyUrnInput';
||||||| 952f3cc3118
import React from 'react';
import { PropertyCardinality, StdDataType, StructuredPropertyEntity } from '@src/types.generated';
import SingleSelectInput from './SingleSelectInput';
import MultiSelectInput from './MultiSelectInput';
import StringInput from './StringInput';
import RichTextInput from './RichTextInput';
import DateInput from './DateInput';
import NumberInput from './NumberInput';
import UrnInput from '../../../entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
=======
import { PropertyCardinality, StdDataType, StructuredPropertyEntity } from '@src/types.generated';
import React from 'react';
import StructuredPropertySearchSelectUrnInput from '../../../entityForm/prompts/StructuredPropertyPrompt/UrnInput/StructuredPropertySearchSelectUrnInput';
import UrnInput from '../../../entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
import DateInput from './DateInput';
import MultiSelectInput from './MultiSelectInput';
import NumberInput from './NumberInput';
import RichTextInput from './RichTextInput';
import SingleSelectInput from './SingleSelectInput';
import StringInput from './StringInput';
>>>>>>> master

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: (string | number | null)[];
    selectSingleValue: (value: string | number) => void;
    toggleSelectedValue: (value: string | number) => void;
    updateSelectedValues: (value: (string | number | null)[]) => void;
<<<<<<< HEAD
    fieldEntity?: Maybe<SchemaFieldEntity>;
||||||| 952f3cc3118
=======
    canUseSearchSelectUrnInput?: boolean;
>>>>>>> master
}

export default function StructuredPropertyInput({
    structuredProperty,
    selectSingleValue,
    selectedValues,
    toggleSelectedValue,
    updateSelectedValues,
<<<<<<< HEAD
    fieldEntity,
||||||| 952f3cc3118
=======
    canUseSearchSelectUrnInput = false,
>>>>>>> master
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
<<<<<<< HEAD
            {!allowedValues && valueType.info.type === StdDataType.Urn && (
                <StructuredPropertyUrnInput
||||||| 952f3cc3118
            {!allowedValues && valueType.info.type === StdDataType.Urn && (
                <UrnInput
=======
            {!allowedValues && valueType.info.type === StdDataType.Urn && canUseSearchSelectUrnInput && (
                <StructuredPropertySearchSelectUrnInput
                    structuredProperty={structuredProperty}
                    selectedValues={selectedValues as string[]}
                    updateSelectedValues={updateSelectedValues}
                />
            )}
            {!allowedValues && valueType.info.type === StdDataType.Urn && !canUseSearchSelectUrnInput && (
                <UrnInput
>>>>>>> master
                    structuredProperty={structuredProperty}
                    selectedValues={selectedValues as string[]}
                    updateSelectedValues={updateSelectedValues}
                    fieldEntity={fieldEntity}
                />
            )}
        </>
    );
}
