import { Binary } from '@phosphor-icons/react/dist/csr/Binary';
import { BracketsCurly } from '@phosphor-icons/react/dist/csr/BracketsCurly';
import { CalendarBlank } from '@phosphor-icons/react/dist/csr/CalendarBlank';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Hash } from '@phosphor-icons/react/dist/csr/Hash';
import { TextT } from '@phosphor-icons/react/dist/csr/TextT';
import { ToggleLeft } from '@phosphor-icons/react/dist/csr/ToggleLeft';
import React from 'react';

import { SchemaFieldDataType } from '@types';

export function ColumnTypeIcon(type?: SchemaFieldDataType): JSX.Element | null {
    if (type === SchemaFieldDataType.Number) {
        return <Hash />;
    }
    if (type === SchemaFieldDataType.String) {
        return <TextT />;
    }
    if (type === SchemaFieldDataType.Date) {
        return <CalendarBlank />;
    }
    if (type === SchemaFieldDataType.Time) {
        return <Clock />;
    }
    if (type === SchemaFieldDataType.Boolean) {
        return <ToggleLeft />;
    }
    if (type === SchemaFieldDataType.Bytes) {
        return <Binary />;
    }
    // Complex / nested types (Struct, Array, Map, Enum, Union, Fixed, Null) share the object-like
    // "{}" icon. Note: Cube is intentionally avoided here since it is the ML Model entity icon.
    return <BracketsCurly />;
}

export function TypeTooltipTitle(type: SchemaFieldDataType, nativeDataType: string | null | undefined) {
    const label = nativeDataType ? `${type} | ${nativeDataType.toLowerCase()}` : type;
    return <span>{label}</span>;
}
