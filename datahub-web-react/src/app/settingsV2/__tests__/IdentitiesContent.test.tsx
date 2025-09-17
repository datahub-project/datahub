import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { IdentitiesContent } from '@app/settingsV2/IdentitiesContent';
import { useIsAppConfigContextLoaded, useIsInviteUsersEnabled } from '@app/useAppConfig';

// Mock the components
vi.mock('@app/identity/ManageIdentities', () => ({
    ManageIdentities: ({ version }: { version: string }) => (
        <div data-testid={`manage-identities-${version}`}>ManageIdentities v{version}</div>
    ),
}));

vi.mock('@app/identity/ManageUsersAndGroups', () => ({
    ManageUsersAndGroups: () => <div data-testid="manage-users-and-groups">ManageUsersAndGroups</div>,
}));

// Mock the feature flag hook
vi.mock('@app/useAppConfig', () => ({
    useIsInviteUsersEnabled: vi.fn(),
    useIsAppConfigContextLoaded: vi.fn(),
}));

describe('IdentitiesContent', () => {
    const mockUseIsInviteUsersEnabled = vi.mocked(useIsInviteUsersEnabled);
    const mockUseIsAppConfigContextLoaded = vi.mocked(useIsAppConfigContextLoaded);

    beforeEach(() => {
        vi.clearAllMocks();
        mockUseIsAppConfigContextLoaded.mockReturnValue(true);
    });

    it('renders ManageUsersAndGroups when inviteUsersEnabled is true', () => {
        mockUseIsInviteUsersEnabled.mockReturnValue(true);

        render(<IdentitiesContent />);

        expect(screen.getByTestId('manage-users-and-groups')).toBeInTheDocument();
        expect(screen.queryByTestId('manage-identities-v2')).not.toBeInTheDocument();
    });

    it('renders ManageIdentities v2 when inviteUsersEnabled is false', () => {
        mockUseIsInviteUsersEnabled.mockReturnValue(false);

        render(<IdentitiesContent />);

        expect(screen.getByTestId('manage-identities-v2')).toBeInTheDocument();
        expect(screen.queryByTestId('manage-users-and-groups')).not.toBeInTheDocument();
    });

    it('ensures resend invitation feature is only available when inviteUsersEnabled=true', () => {
        // When feature flag is false, we use ManageIdentities which doesn't have resend invitation
        mockUseIsInviteUsersEnabled.mockReturnValue(false);
        render(<IdentitiesContent />);
        expect(screen.getByTestId('manage-identities-v2')).toBeInTheDocument();

        // When feature flag is true, we use ManageUsersAndGroups which has resend invitation
        mockUseIsInviteUsersEnabled.mockReturnValue(true);
        render(<IdentitiesContent />);
        expect(screen.getByTestId('manage-users-and-groups')).toBeInTheDocument();
    });
});
