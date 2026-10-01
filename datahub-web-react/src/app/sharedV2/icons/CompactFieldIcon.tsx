import { Binary } from '@phosphor-icons/react/dist/csr/Binary';
import { CalendarBlank } from '@phosphor-icons/react/dist/csr/CalendarBlank';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Hash } from '@phosphor-icons/react/dist/csr/Hash';
import { IdentificationCard } from '@phosphor-icons/react/dist/csr/IdentificationCard';
import { TextAa } from '@phosphor-icons/react/dist/csr/TextAa';
import { TextB } from '@phosphor-icons/react/dist/csr/TextB';
import { Tooltip } from '@components';
import React from 'react';

import { SchemaFieldDataType } from '@types';

function CompactFieldIcon(type?: SchemaFieldDataType): JSX.Element | null {
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

export function CompactFieldIconWithTooltip({
    type,
    nativeDataType,
}: {
    type: SchemaFieldDataType;
    nativeDataType: string | null | undefined;
}): JSX.Element {
    return (
        <Tooltip showArrow={false} placement="left" title={TypeTooltipTitle(type, nativeDataType)}>
            {CompactFieldIcon(type)}
        </Tooltip>
    );
}

function TypeTooltipTitle(type: SchemaFieldDataType, nativeDataType: string | null | undefined) {
    const label = (type === SchemaFieldDataType.Null && nativeDataType) || type;
    return <span style={{ textTransform: 'capitalize' }}>{label.toLowerCase()}</span>;
}
