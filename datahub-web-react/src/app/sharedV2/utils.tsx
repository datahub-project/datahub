import { Binary } from '@phosphor-icons/react/dist/csr/Binary';
import { CalendarBlank } from '@phosphor-icons/react/dist/csr/CalendarBlank';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Hash } from '@phosphor-icons/react/dist/csr/Hash';
import { IdentificationCard } from '@phosphor-icons/react/dist/csr/IdentificationCard';
import { TextAa } from '@phosphor-icons/react/dist/csr/TextAa';
import { TextB } from '@phosphor-icons/react/dist/csr/TextB';
import React from 'react';

import { SchemaFieldDataType } from '@types';

export function ColumnTypeIcon(type?: SchemaFieldDataType): JSX.Element | null {
    if (type === SchemaFieldDataType.Number) {
        return <Hash />;
    }
    if (type === SchemaFieldDataType.String) {
        return <TextAa />;
    }
    if (type === SchemaFieldDataType.Date) {
        return <CalendarBlank />;
    }
    if (type === SchemaFieldDataType.Time) {
        return <Clock />;
    }
    if (type === SchemaFieldDataType.Boolean) {
        return <TextB />;
    }
    if (type === SchemaFieldDataType.Bytes) {
        return <Binary />;
    }
    return <IdentificationCard />;
}

export function TypeTooltipTitle(type: SchemaFieldDataType, nativeDataType: string | null | undefined) {
    const label = nativeDataType ? `${type} | ${nativeDataType.toLowerCase()}` : type;
    return <span>{label}</span>;
}
