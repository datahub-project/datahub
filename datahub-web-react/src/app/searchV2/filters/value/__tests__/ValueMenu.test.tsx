import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FieldType, FilterField, FilterValue } from '@app/searchV2/filters/types';
import { FilterMenuProvider, useFilterMenuCommits } from '@app/searchV2/filters/value/FilterMenuContext';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import themeV2 from '@conf/theme/themeV2';

const { renderState } = vi.hoisted(() => ({ renderState: { isRendered: true } }));

vi.mock('@app/searchV2/filters/value/useIsRendered', () => ({
    useIsRendered: () => renderState.isRendered,
}));

const field: FilterField = { field: 'description', displayName: 'Description', type: FieldType.TEXT };

function renderPopoverMenu(values: FilterValue[], onChangeValues: (v: FilterValue[]) => void) {
    return (
        <ThemeProvider theme={themeV2}>
            <ValueMenu field={field} values={values} defaultOptions={[]} onChangeValues={onChangeValues} />
        </ThemeProvider>
    );
}

// Stands in for Add filter or More: closing the parent menu commits its submenus.
function ParentMenu({ onChangeValues, onClose }: { onChangeValues: (v: FilterValue[]) => void; onClose: () => void }) {
    const { registerCommit, commitAll } = useFilterMenuCommits();
    const commitAndClose = (applyNow?: () => void) => {
        applyNow?.();
        commitAll();
        onClose();
    };
    return (
        <ThemeProvider theme={themeV2}>
            <FilterMenuProvider value={{ registerCommit, commitAndClose }}>
                <ValueMenu field={field} values={[]} defaultOptions={[]} onChangeValues={onChangeValues} />
                <button type="button" onClick={() => commitAndClose()}>
                    close parent
                </button>
            </FilterMenuProvider>
        </ThemeProvider>
    );
}

describe('ValueMenu', () => {
    beforeEach(() => {
        renderState.isRendered = true;
    });

    describe('in a popover', () => {
        it('commits staged values when the menu hides', () => {
            const onChangeValues = vi.fn();
            const { rerender } = render(renderPopoverMenu([], onChangeValues));

            fireEvent.change(screen.getByRole('textbox'), { target: { value: 'orders' } });
            renderState.isRendered = false;
            rerender(renderPopoverMenu([], onChangeValues));

            expect(onChangeValues).toHaveBeenCalledWith([{ value: 'orders', entity: null }]);
        });

        it('does not write the previous values back when the menu hides as the committed values arrive', () => {
            const onChangeValues = vi.fn();
            const { rerender } = render(renderPopoverMenu([], onChangeValues));

            const input = screen.getByRole('textbox');
            fireEvent.change(input, { target: { value: 'orders' } });
            fireEvent.keyDown(input, { key: 'Enter' });

            renderState.isRendered = false;
            rerender(renderPopoverMenu([{ value: 'orders', entity: null }], onChangeValues));

            expect(onChangeValues).toHaveBeenCalledTimes(1);
        });
    });

    describe('in a submenu', () => {
        it('keeps staged values when the submenu hides, and commits them when the parent closes', () => {
            const onChangeValues = vi.fn();
            const onClose = vi.fn();
            const { rerender } = render(<ParentMenu onChangeValues={onChangeValues} onClose={onClose} />);

            fireEvent.change(screen.getByRole('textbox'), { target: { value: 'orders' } });
            renderState.isRendered = false;
            rerender(<ParentMenu onChangeValues={onChangeValues} onClose={onClose} />);
            expect(onChangeValues).not.toHaveBeenCalled();

            fireEvent.click(screen.getByRole('button', { name: 'close parent' }));
            expect(onChangeValues).toHaveBeenCalledTimes(1);
            expect(onChangeValues).toHaveBeenCalledWith([{ value: 'orders', entity: null }]);
        });

        it('commits once on Enter and closes the parent menu', () => {
            const onChangeValues = vi.fn();
            const onClose = vi.fn();
            render(<ParentMenu onChangeValues={onChangeValues} onClose={onClose} />);

            const input = screen.getByRole('textbox');
            fireEvent.change(input, { target: { value: 'orders' } });
            fireEvent.keyDown(input, { key: 'Enter' });

            expect(onChangeValues).toHaveBeenCalledTimes(1);
            expect(onClose).toHaveBeenCalled();
        });
    });
});
