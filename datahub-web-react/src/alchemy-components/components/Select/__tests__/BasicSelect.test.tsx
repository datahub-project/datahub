import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it } from 'vitest';

import { Select } from '@components/components/Select/Select';

import themeV2 from '@conf/theme/themeV2';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const OPTIONS = [
    { value: 'asset-a', label: 'Customer Orders' },
    { value: 'asset-b', label: 'Important Asset' },
];

beforeEach(mockVisibilityObserver);

function renderSelect(filterResultsByQuery?: boolean) {
    render(
        <ThemeProvider theme={themeV2}>
            <Select
                options={OPTIONS}
                placeholder="Select an asset"
                showSearch
                filterResultsByQuery={filterResultsByQuery}
            />
        </ThemeProvider>,
    );

    fireEvent.click(screen.getByText('Select an asset'));
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'customer' } });
}

describe('BasicSelect search filtering', () => {
    it('filters options by their labels by default', () => {
        renderSelect();

        expect(screen.getByText('Customer Orders')).toBeInTheDocument();
        expect(screen.queryByText('Important Asset')).not.toBeInTheDocument();
    });

    it('keeps server search results when client filtering is disabled', () => {
        renderSelect(false);

        expect(screen.getByText('Customer Orders')).toBeInTheDocument();
        expect(screen.getByText('Important Asset')).toBeInTheDocument();
    });
});
