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

function renderIconHtml(node: JSX.Element | null) {
    return render(node as JSX.Element).container.innerHTML;
}

describe('ColumnTypeIcon', () => {
    it('should return TextAUnderline for String type', () => {
        expect(renderIconHtml(ColumnTypeIcon(SchemaFieldDataType.String))).toBe(renderIconHtml(<TextAUnderline />));
    });

    it('should return CalendarBlank for Date type', () => {
        expect(renderIconHtml(ColumnTypeIcon(SchemaFieldDataType.Date))).toBe(renderIconHtml(<CalendarBlank />));
    });

    it('should return Clock for Time type', () => {
        expect(renderIconHtml(ColumnTypeIcon(SchemaFieldDataType.Time))).toBe(renderIconHtml(<Clock />));
    });

    it('should return TextB for Boolean type', () => {
        expect(renderIconHtml(ColumnTypeIcon(SchemaFieldDataType.Boolean))).toBe(renderIconHtml(<TextB />));
    });

    it('should return Binary for Bytes type', () => {
        expect(renderIconHtml(ColumnTypeIcon(SchemaFieldDataType.Bytes))).toBe(renderIconHtml(<Binary />));
    });

    it('should return Question for unknown type', () => {
        expect(renderIconHtml(ColumnTypeIcon(undefined))).toBe(renderIconHtml(<Question />));
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
