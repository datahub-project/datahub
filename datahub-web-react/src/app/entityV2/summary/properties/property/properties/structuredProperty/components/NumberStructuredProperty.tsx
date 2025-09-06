import React, { useMemo } from 'react';

import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import TextValue from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/TextValue';
import { StructuredPropertyComponentProps } from '@app/entityV2/summary/properties/property/properties/structuredProperty/types';

import { NumberValue } from '@types';

export default function NumberStructuredProperty({
    structuredPropertyEntry,
    ...props
}: StructuredPropertyComponentProps) {
    const values = useMemo(
        () => structuredPropertyEntry?.values.map((value) => (value as NumberValue)?.numberValue) ?? [],
        [structuredPropertyEntry?.values],
    );

    return (
        <BaseProperty
            {...props}
            values={values}
            renderValue={(value) => <TextValue text={`${value}`} maxWidth="100px" disableWrapping />}
            renderValueInTooltip={(value) => <TextValue text={`${value}`} />}
        />
    );
}
