import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { type MockedFunction, vi } from 'vitest';

import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { checkIsSsoConfigured } from '@app/settingsV2/platform/sso/utils';

import { GetInviteTokenDocument, ListRolesDocument } from '@graphql/role.generated';
import { GetSsoSettingsDocument } from '@graphql/settings.generated';

// Mock the checkIsSsoConfigured function
vi.mock('@app/settingsV2/platform/sso/utils', () => ({
    checkIsSsoConfigured: vi.fn(),
}));

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: { event: vi.fn() },
    EventType: { CreateInviteLinkEvent: 'CreateInviteLinkEvent' },
}));

// Mock data definitions
const mockSsoSettings = {
    request: {
        query: GetSsoSettingsDocument,
    },
    result: {
        data: {
            globalSettings: {
                ssoSettings: null, // No SSO configured
            },
        },
    },
};

const mockSsoSettingsConfigured = {
    request: {
        query: GetSsoSettingsDocument,
    },
    result: {
        data: {
            globalSettings: {
                ssoSettings: {
                    enabled: true,
                    // Mock configured SSO
                },
            },
        },
    },
};

const mockRolesQuery = {
    request: {
        query: ListRolesDocument,
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    },
    result: {
        data: {
            listRoles: {
                start: 0,
                count: 2,
                total: 2,
                roles: [
                    {
                        urn: 'urn:li:role:reader',
                        name: 'Reader',
                        type: 'CORP_USER',
                        __typename: 'DataHubRole',
                    },
                    {
                        urn: 'urn:li:role:editor',
                        name: 'Editor',
                        type: 'CORP_USER',
                        __typename: 'DataHubRole',
                    },
                ],
            },
        },
    },
};

const mockInviteTokenQuery = {
    request: {
        query: GetInviteTokenDocument,
        variables: {
            input: {
                roleUrn: undefined,
            },
        },
    },
    result: {
        data: {
            getInviteToken: {
                inviteToken: 'test-token-123',
            },
        },
    },
};

describe('ViewInviteTokenModal', () => {
    const defaultProps = {
        open: true,
        onClose: vi.fn(),
    };

    const mockCheckIsSsoConfigured = checkIsSsoConfigured as MockedFunction<typeof checkIsSsoConfigured>;

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('SSO configuration conditional rendering', () => {
        it('should show SSO setup section when SSO is NOT configured', async () => {
            // Mock SSO as NOT configured
            (mockCheckIsSsoConfigured as any).mockReturnValue(false);

            render(
                <MockedProvider mocks={[mockSsoSettings, mockRolesQuery, mockInviteTokenQuery]}>
                    <MemoryRouter>
                        <ViewInviteTokenModal {...defaultProps} />
                    </MemoryRouter>
                </MockedProvider>,
            );

            // Should show the SSO setup section
            expect(await screen.findByText('Or, set up Single Sign-On')).toBeInTheDocument();
            expect(
                screen.getByText(
                    'Setting up SSO allows teammates within your organization to sign up with their existing accounts.',
                ),
            ).toBeInTheDocument();
            expect(screen.getByText('Configure SSO')).toBeInTheDocument();
        });

        it('should NOT show SSO setup section when SSO is already configured', async () => {
            // Mock SSO as configured
            (mockCheckIsSsoConfigured as any).mockReturnValue(true);

            render(
                <MockedProvider mocks={[mockSsoSettingsConfigured, mockRolesQuery, mockInviteTokenQuery]}>
                    <MemoryRouter>
                        <ViewInviteTokenModal {...defaultProps} />
                    </MemoryRouter>
                </MockedProvider>,
            );

            // Wait for the modal to render
            expect(await screen.findByText('Share Invite Link')).toBeInTheDocument();

            // Should NOT show the SSO setup section
            expect(screen.queryByText('Or, set up Single Sign-On')).not.toBeInTheDocument();
            expect(
                screen.queryByText(
                    'Setting up SSO allows teammates within your organization to sign up with their existing accounts.',
                ),
            ).not.toBeInTheDocument();
            expect(screen.queryByText('Configure SSO')).not.toBeInTheDocument();
        });
    });

    describe('modal basic functionality', () => {
        it('should render modal when open is true', async () => {
            (mockCheckIsSsoConfigured as any).mockReturnValue(false);

            render(
                <MockedProvider mocks={[mockSsoSettings, mockRolesQuery, mockInviteTokenQuery]}>
                    <MemoryRouter>
                        <ViewInviteTokenModal {...defaultProps} />
                    </MemoryRouter>
                </MockedProvider>,
            );

            expect(await screen.findByText('Share Invite Link')).toBeInTheDocument();
            expect(
                screen.getByText(
                    'Copy an invite link to send to your users. When they join, users will be automatically assigned to the selected role.',
                ),
            ).toBeInTheDocument();
        });

        it('should not render modal when open is false', () => {
            (mockCheckIsSsoConfigured as any).mockReturnValue(false);

            render(
                <MockedProvider mocks={[mockSsoSettings, mockRolesQuery, mockInviteTokenQuery]}>
                    <MemoryRouter>
                        <ViewInviteTokenModal {...defaultProps} open={false} />
                    </MemoryRouter>
                </MockedProvider>,
            );

            expect(screen.queryByText('Share Invite Link')).not.toBeInTheDocument();
        });
    });
});
