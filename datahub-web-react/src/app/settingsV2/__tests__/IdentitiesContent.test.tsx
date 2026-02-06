import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { IdentitiesContent } from '@app/settingsV2/IdentitiesContent';

// Mock ManageUsersAndGroups component
vi.mock('@app/identity/ManageUsersAndGroups', () => ({
    ManageUsersAndGroups: ({ version }: { version?: string }) => (
        <div data-testid="manage-users-and-groups" data-version={version}>
            ManageUsersAndGroups
        </div>
    ),
}));

describe('IdentitiesContent', () => {
    it('always renders ManageUsersAndGroups', () => {
        render(<IdentitiesContent />);

        expect(screen.getByTestId('manage-users-and-groups')).toBeInTheDocument();
        expect(screen.getByTestId('manage-users-and-groups')).toHaveAttribute('data-version', 'v2');
    });
});
