import { Binary } from '@phosphor-icons/react/dist/csr/Binary';
import { BracketsCurly } from '@phosphor-icons/react/dist/csr/BracketsCurly';
import { BracketsSquare } from '@phosphor-icons/react/dist/csr/BracketsSquare';
import { CalendarBlank } from '@phosphor-icons/react/dist/csr/CalendarBlank';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Empty } from '@phosphor-icons/react/dist/csr/Empty';
import { Hash } from '@phosphor-icons/react/dist/csr/Hash';
import { Key } from '@phosphor-icons/react/dist/csr/Key';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { TextAUnderline } from '@phosphor-icons/react/dist/csr/TextAUnderline';
import { TextB } from '@phosphor-icons/react/dist/csr/TextB';
import React from 'react';

import { SchemaFieldDataType } from '@types';

export function ColumnTypeIcon(type?: SchemaFieldDataType): JSX.Element | null {
    if (type === SchemaFieldDataType.Number) {
        return <Hash />;
    }
    if (type === SchemaFieldDataType.String) {
        return <TextAUnderline />;
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
    if (type === SchemaFieldDataType.Struct) {
        return <BracketsCurly />;
    }
    if (type === SchemaFieldDataType.Array) {
        return <BracketsSquare />;
    }
    if (type === SchemaFieldDataType.Map) {
        return <Key />;
    }
    if (type === SchemaFieldDataType.Null) {
        return <Empty />;
    }
    return <Question />;
}

export function TypeTooltipTitle(type: SchemaFieldDataType, nativeDataType: string | null | undefined) {
    const label = nativeDataType ? `${type} | ${nativeDataType.toLowerCase()}` : type;
    return <span>{label}</span>;
}
