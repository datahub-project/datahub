import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { BooleanSimpleSearchFilter } from '@app/searchV2/filters/render/shared/BooleanSimpleSearchFilter';
import themeV2 from '@conf/theme/themeV2';

vi.mock('@app/searchV2/SearchFilterLabel', () => ({
    SearchFilterLabel: ({ value }: { value: string }) => <span>{value}</span>,
}));

const renderFilter = (onSelect: () => void) =>
    render(
        <ThemeProvider theme={themeV2}>
            <BooleanSimpleSearchFilter
                title="Deprecation"
                option="Is Deprecated"
                count={3}
                isSelected={false}
                onSelect={onSelect}
                defaultDisplayFilters
            />
        </ThemeProvider>,
    );

describe('BooleanSimpleSearchFilter', () => {
    it('selects when the option text is clicked', () => {
        const onSelect = vi.fn();
        renderFilter(onSelect);

        fireEvent.click(screen.getByText('Is Deprecated'));

        expect(onSelect).toHaveBeenCalledTimes(1);
    });

    it('selects once when the checkbox itself is clicked', () => {
        const onSelect = vi.fn();
        renderFilter(onSelect);

        fireEvent.click(screen.getByTestId('checkbox-base'));

        expect(onSelect).toHaveBeenCalledTimes(1);
    });
});
