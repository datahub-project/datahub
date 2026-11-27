import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { Route } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ManagePermissions } from '@app/permissions/ManagePermissions';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const mockUseAppConfig = vi.hoisted(() => vi.fn());

vi.mock('@app/useAppConfig', () => ({ useAppConfig: () => mockUseAppConfig() }));
vi.mock('@app/permissions/roles/ManageRoles', () => ({ ManageRoles: () => <div>roles content</div> }));
vi.mock('@app/permissions/policy/ManagePolicies', () => ({ ManagePolicies: () => <div>policies content</div> }));
vi.mock('@app/permissions/dataAccessRoles/ManageDataAccessRoles', () => ({
    ManageDataAccessRoles: () => <div>data access roles content</div>,
}));

function renderPage() {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer initialEntries={['/permissions']}>
                <Route path="/permissions">
                    <ManagePermissions />
                </Route>
            </TestPageContainer>
        </MockedProvider>,
    );
}

function withFlag(showAccessManagement?: boolean) {
    mockUseAppConfig.mockReturnValue({
        config: { featureFlags: { showAccessManagement }, visualConfig: { theme: { themeId: null } } },
        loaded: true,
    });
}

describe('ManagePermissions', () => {
    beforeEach(() => {
        mockUseAppConfig.mockReset();
    });

    it.each([false, undefined])('hides the Data Access Roles tab when the feature flag is %s', (flag) => {
        withFlag(flag);
        renderPage();

        expect(screen.getByText('Policies')).toBeInTheDocument();
        expect(screen.queryByText('Data Access Roles')).not.toBeInTheDocument();
    });

    it('shows the Data Access Roles tab when the feature flag is on and still defaults to Roles', () => {
        withFlag(true);
        renderPage();

        expect(screen.getByText('Data Access Roles')).toBeInTheDocument();
        expect(screen.getByText('roles content')).toBeInTheDocument();
        expect(screen.queryByText('data access roles content')).not.toBeInTheDocument();
    });
});
