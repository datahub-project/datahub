/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { render } from '@testing-library/react';

import { ColumnTypeIcon, TypeTooltipTitle } from '@app/sharedV2/utils';

import { SchemaFieldDataType } from '@types';

describe('ColumnTypeIcon', () => {
    it('should return FontColorsOutlined for String type', () => {
        const { container } = render(ColumnTypeIcon(SchemaFieldDataType.String) as JSX.Element);
        expect(container.querySelector('.anticon-font-colors')).toBeInTheDocument();
    });

    it('should return CalendarOutlined for Date type', () => {
        const { container } = render(ColumnTypeIcon(SchemaFieldDataType.Date) as JSX.Element);
        expect(container.querySelector('.anticon-calendar')).toBeInTheDocument();
    });

    it('should return ClockCircleOutlined for Time type', () => {
        const { container } = render(ColumnTypeIcon(SchemaFieldDataType.Time) as JSX.Element);
        expect(container.querySelector('.anticon-clock-circle')).toBeInTheDocument();
    });

    it('should return BoldOutlined for Boolean type', () => {
        const { container } = render(ColumnTypeIcon(SchemaFieldDataType.Boolean) as JSX.Element);
        expect(container.querySelector('.anticon-bold')).toBeInTheDocument();
    });

    it('should return FieldBinaryOutlined for Bytes type', () => {
        const { container } = render(ColumnTypeIcon(SchemaFieldDataType.Bytes) as JSX.Element);
        expect(container.querySelector('.anticon-field-binary')).toBeInTheDocument();
    });

    it('should return ProfileOutlined for unknown type', () => {
        const { container } = render(ColumnTypeIcon(undefined) as JSX.Element);
        expect(container.querySelector('.anticon-profile')).toBeInTheDocument();
    });
});

describe('TypeTooltipTitle', () => {
    it('should display type and nativeDataType in tooltip title', () => {
        const type = SchemaFieldDataType.String;
        const nativeDataType = 'VARCHAR';
        const { container } = render(TypeTooltipTitle(type, nativeDataType) as JSX.Element);
        expect(container.textContent).toBe(`${type} | ${nativeDataType.toLowerCase()}`);
    });

    it('should display only type if nativeDataType is null', () => {
        const type = SchemaFieldDataType.Date;
        const { container } = render(TypeTooltipTitle(type, null) as JSX.Element);
        expect(container.textContent).toBe(type);
    });
});
