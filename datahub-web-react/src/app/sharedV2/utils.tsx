import React from 'react';
import {
    BoldOutlined,
    CalendarOutlined,
    ClockCircleOutlined,
    FieldBinaryOutlined,
    FontColorsOutlined,
    NumberOutlined,
    ProfileOutlined,
} from '@ant-design/icons';
import { SchemaFieldDataType } from '../../types.generated';

export function ColumnTypeIcon(type?: SchemaFieldDataType): JSX.Element | null {
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

export function TypeTooltipTitle(type: SchemaFieldDataType, nativeDataType: string | null | undefined) {
    const label = nativeDataType ? `${type} | ${nativeDataType.toLowerCase()}` : type;
    return <span>{label}</span>;
}
