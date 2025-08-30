import React from 'react';
import { render, screen } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

vi.mock('@graphql/role.generated', () => ({
    useListRolesQuery: () => ({ data: { listRoles: { roles: [] } } }),
    useGetInviteTokenQuery: () => ({ data: { getInviteToken: { inviteToken: 'abc123' } } }),
}));

vi.mock('@graphql/mutations.generated', () => ({
    useCreateInviteTokenMutation: () => [() => Promise.resolve({ data: { createInviteToken: { inviteToken: 'new123' } } })],
}));

import InviteUsersModal from '@app/identityV2/user/InviteUsersModal';

describe('InviteUsersModal', () => {
    it('renders with title and copy button', () => {
        render(
            <MockedProvider>
                <InviteUsersModal open onClose={() => {}} />
            </MockedProvider>,
        );

        expect(screen.getByText('Invite Users')).toBeInTheDocument();
        expect(screen.getByText('Copy')).toBeInTheDocument();
        expect(screen.getByText('Refresh')).toBeInTheDocument();
    });
});

