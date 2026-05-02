import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import BrowseSidebar from '@app/searchV2/sidebar/BrowseSidebar';
import { BrowseProvider } from '@app/searchV2/sidebar/BrowseContext';
import { useBrowseSortOrder } from '@app/searchV2/sidebar/BrowseSortContext';
import { sortBrowseResultGroups } from '@app/searchV2/sidebar/browseSortUtils';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/searchV2/useSearchAndBrowseVersion', () => ({
    useIsPlatformBrowseV2: () => false,
}));

vi.mock('@app/searchV2/sidebar/PlatformBrowse', () => ({
    default: () => null,
}));

vi.mock('@app/searchV2/sidebar/EntityBrowse', () => ({
    default: () => {
        const sortOrder = useBrowseSortOrder();
        const groups = sortBrowseResultGroups(
            [
                { name: 'c-folder', count: 1, hasSubGroups: false, entity: null },
                { name: 'a-folder', count: 1, hasSubGroups: false, entity: null },
                { name: 'b-folder', count: 1, hasSubGroups: false, entity: null },
            ],
            sortOrder,
            {
                getDisplayName: () => '',
            } as any,
        );

        return (
            <div data-testid="browse-order">
                {groups.map((group) => group.name).join(', ')}
            </div>
        );
    },
}));

function renderSidebar() {
    return render(
        <TestPageContainer>
            <BrowseProvider>
                <BrowseSidebar visible />
            </BrowseProvider>
        </TestPageContainer>,
    );
}

describe('BrowseSidebar sort toggle', () => {
    it('renders A-Z by default and lets users switch between sort modes', async () => {
        renderSidebar();

        expect(screen.getByTestId('browse-order')).toHaveTextContent('a-folder, b-folder, c-folder');

        fireEvent.click(screen.getByTestId('browse-sort-toggle'));
        fireEvent.click(await screen.findByText('Alphabetical (Z-A)'));
        expect(screen.getByTestId('browse-order')).toHaveTextContent('c-folder, b-folder, a-folder');

        fireEvent.click(screen.getByTestId('browse-sort-toggle'));
        fireEvent.click(await screen.findByText('Recently Used'));
        expect(screen.getByTestId('browse-order')).toHaveTextContent('c-folder, a-folder, b-folder');
    });
});
