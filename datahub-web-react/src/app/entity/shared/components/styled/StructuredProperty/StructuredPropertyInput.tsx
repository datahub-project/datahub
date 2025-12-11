/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import DateInput from '@app/entity/shared/components/styled/StructuredProperty/DateInput';
import MultiSelectInput from '@app/entity/shared/components/styled/StructuredProperty/MultiSelectInput';
import NumberInput from '@app/entity/shared/components/styled/StructuredProperty/NumberInput';
import RichTextInput from '@app/entity/shared/components/styled/StructuredProperty/RichTextInput';
import SingleSelectInput from '@app/entity/shared/components/styled/StructuredProperty/SingleSelectInput';
import StringInput from '@app/entity/shared/components/styled/StructuredProperty/StringInput';
import StructuredPropertySearchSelectUrnInput from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/StructuredPropertySearchSelectUrnInput';
import UrnInput from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
import { PropertyCardinality, StdDataType, StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: (string | number | null)[];
    selectSingleValue: (value: string | number) => void;
    toggleSelectedValue: (value: string | number) => void;
    updateSelectedValues: (value: (string | number | null)[]) => void;
    canUseSearchSelectUrnInput?: boolean;
}

export default function StructuredPropertyInput({
    structuredProperty,
    selectSingleValue,
    selectedValues,
    toggleSelectedValue,
    updateSelectedValues,
    canUseSearchSelectUrnInput = false,
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
            {!allowedValues && valueType.info.type === StdDataType.Urn && canUseSearchSelectUrnInput && (
                <StructuredPropertySearchSelectUrnInput
                    structuredProperty={structuredProperty}
                    selectedValues={selectedValues as string[]}
                    updateSelectedValues={updateSelectedValues}
                />
            )}
            {!allowedValues && valueType.info.type === StdDataType.Urn && !canUseSearchSelectUrnInput && (
                <UrnInput
                    structuredProperty={structuredProperty}
                    selectedValues={selectedValues as string[]}
                    updateSelectedValues={updateSelectedValues}
                />
            )}
        </>
    );
}
