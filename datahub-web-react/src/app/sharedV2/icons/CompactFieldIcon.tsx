import React from 'react';
import { Tooltip } from '@components';
import {
    BoldOutlined,
    CalendarOutlined,
    ClockCircleOutlined,
    FieldBinaryOutlined,
    FontColorsOutlined,
    NumberOutlined,
    ProfileOutlined,
} from '@ant-design/icons';
import { SchemaFieldDataType } from '../../../types.generated';

export default function CompactFieldIcon(type?: SchemaFieldDataType): JSX.Element | null {
    if (type === SchemaFieldDataType.Number) {
        return <NumberOutlined />;
    }
    if (type === SchemaFieldDataType.String) {
        return <FontColorsOutlined />;
    }
    if (type === SchemaFieldDataType.Date) {
        return <CalendarOutlined />;
    }
    if (type === SchemaFieldDataType.Time) {
        return <ClockCircleOutlined />;
    }
    if (type === SchemaFieldDataType.Boolean) {
        return <BoldOutlined />;
    }
    if (type === SchemaFieldDataType.Bytes) {
        return <FieldBinaryOutlined />;
    }
    return <ProfileOutlined />;
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
