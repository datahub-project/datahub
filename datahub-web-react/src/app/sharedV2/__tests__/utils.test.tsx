import { Binary } from '@phosphor-icons/react/dist/csr/Binary';
import { CalendarBlank } from '@phosphor-icons/react/dist/csr/CalendarBlank';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { TextAUnderline } from '@phosphor-icons/react/dist/csr/TextAUnderline';
import { TextB } from '@phosphor-icons/react/dist/csr/TextB';
import { render } from '@testing-library/react';
import React from 'react';

import { ColumnTypeIcon, TypeTooltipTitle } from '@app/sharedV2/utils';

import { SchemaFieldDataType } from '@types';

function expectIconMatch(actual: JSX.Element | null, expected: JSX.Element) {
    const { container: actualContainer } = render(actual as JSX.Element);
    const { container: expectedContainer } = render(expected);
    expect(actualContainer.innerHTML).toBe(expectedContainer.innerHTML);
}

describe('ColumnTypeIcon', () => {
    it('should return TextAUnderline for String type', () => {
        expectIconMatch(ColumnTypeIcon(SchemaFieldDataType.String), <TextAUnderline />);
    });

    it('should return CalendarBlank for Date type', () => {
        expectIconMatch(ColumnTypeIcon(SchemaFieldDataType.Date), <CalendarBlank />);
    });

    it('should return Clock for Time type', () => {
        expectIconMatch(ColumnTypeIcon(SchemaFieldDataType.Time), <Clock />);
    });

    it('should return TextB for Boolean type', () => {
        expectIconMatch(ColumnTypeIcon(SchemaFieldDataType.Boolean), <TextB />);
    });

    it('should return Binary for Bytes type', () => {
        expectIconMatch(ColumnTypeIcon(SchemaFieldDataType.Bytes), <Binary />);
    });

    it('should return Question for unknown type', () => {
        expectIconMatch(ColumnTypeIcon(undefined), <Question />);
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
