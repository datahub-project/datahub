import { render } from '@testing-library/react';
import { ColumnTypeIcon, TypeTooltipTitle } from '../utils';
import { SchemaFieldDataType } from '../../../types.generated';

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
