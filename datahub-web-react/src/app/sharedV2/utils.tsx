/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    BoldOutlined,
    CalendarOutlined,
    ClockCircleOutlined,
    FieldBinaryOutlined,
    FontColorsOutlined,
    NumberOutlined,
    ProfileOutlined,
} from '@ant-design/icons';
import React from 'react';

import { SchemaFieldDataType } from '@types';

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
