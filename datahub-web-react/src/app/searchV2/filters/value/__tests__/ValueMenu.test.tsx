import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FieldType, FilterField, FilterValue } from '@app/searchV2/filters/types';
import { CloseFilterMenuProvider } from '@app/searchV2/filters/value/CloseFilterMenuContext';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import themeV2 from '@conf/theme/themeV2';

const { renderState } = vi.hoisted(() => ({ renderState: { isRendered: true } }));

vi.mock('@app/searchV2/filters/value/useIsRendered', () => ({
    useIsRendered: () => renderState.isRendered,
}));

const field: FilterField = { field: 'description', displayName: 'Description', type: FieldType.TEXT };

function renderMenu(values: FilterValue[], onChangeValues: (v: FilterValue[]) => void, closeMenu = vi.fn()) {
    return (
        <ThemeProvider theme={themeV2}>
            <CloseFilterMenuProvider value={closeMenu}>
                <ValueMenu field={field} values={values} defaultOptions={[]} onChangeValues={onChangeValues} />
            </CloseFilterMenuProvider>
        </ThemeProvider>
    );
}

describe('ValueMenu', () => {
    beforeEach(() => {
        renderState.isRendered = true;
    });

    it('commits on Enter and closes the menu', () => {
        const onChangeValues = vi.fn();
        const closeMenu = vi.fn();
        render(renderMenu([], onChangeValues, closeMenu));

        const input = screen.getByRole('textbox');
        fireEvent.change(input, { target: { value: 'orders' } });
        fireEvent.keyDown(input, { key: 'Enter' });

        expect(onChangeValues).toHaveBeenCalledTimes(1);
        expect(onChangeValues).toHaveBeenCalledWith([{ value: 'orders', entity: null }]);
        expect(closeMenu).toHaveBeenCalled();
    });

    it('does not write the previous values back when the menu hides as the committed values arrive', () => {
        const onChangeValues = vi.fn();
        const { rerender } = render(renderMenu([], onChangeValues));

        const input = screen.getByRole('textbox');
        fireEvent.change(input, { target: { value: 'orders' } });
        fireEvent.keyDown(input, { key: 'Enter' });

        renderState.isRendered = false;
        rerender(renderMenu([{ value: 'orders', entity: null }], onChangeValues));

        expect(onChangeValues).toHaveBeenCalledTimes(1);
    });

    it('commits staged values when the menu hides, without closing it', () => {
        const onChangeValues = vi.fn();
        const closeMenu = vi.fn();
        const { rerender } = render(renderMenu([], onChangeValues, closeMenu));

        fireEvent.change(screen.getByRole('textbox'), { target: { value: 'orders' } });
        renderState.isRendered = false;
        rerender(renderMenu([], onChangeValues, closeMenu));

        expect(onChangeValues).toHaveBeenCalledWith([{ value: 'orders', entity: null }]);
        expect(closeMenu).not.toHaveBeenCalled();
    });
});
