import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { ActionsColumn } from '@app/entityV2/view/select/ViewsTableColumns';

import { DataHubViewType } from '@types';

vi.mock('@app/entityV2/view/menu/ViewDropdownMenu', () => ({
    ViewDropdownMenu: ({ isOwnedByUser }: { isOwnedByUser?: boolean }) => (
        <div data-testid="view-dropdown-menu" data-is-owned-by-user={String(isOwnedByUser)} />
    ),
}));

describe('ActionsColumn', () => {
    it('marks a Personal view as owned by the current user', () => {
        render(<ActionsColumn record={{ urn: 'urn:li:dataHubView:1', viewType: DataHubViewType.Personal }} />);

        expect(screen.getByTestId('view-dropdown-menu')).toHaveAttribute('data-is-owned-by-user', 'true');
    });

    it('does not mark a Global view as owned by the current user', () => {
        render(<ActionsColumn record={{ urn: 'urn:li:dataHubView:2', viewType: DataHubViewType.Global }} />);

        expect(screen.getByTestId('view-dropdown-menu')).toHaveAttribute('data-is-owned-by-user', 'false');
    });
});
