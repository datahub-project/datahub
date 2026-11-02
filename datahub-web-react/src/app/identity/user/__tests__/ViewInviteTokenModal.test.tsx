import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { render, screen } from '@utils/test-utils/customRender';

const mockUserContext = {
    platformPrivileges: {
        managePolicies: false,
    },
};

const getInviteToken = vi.fn();

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => mockUserContext,
}));

vi.mock('@app/identity/user/SimpleSelectRole', () => ({
    default: () => <div data-testid="role-picker" />,
}));

vi.mock('@graphql/role.generated', () => ({
    useGetInviteTokenQuery: (options: { variables?: { input?: { roleUrn?: string } } }) => {
        getInviteToken(options);
        return { data: { getInviteToken: { inviteToken: 'invite-token' } } };
    },
}));

vi.mock('@graphql/mutations.generated', () => ({
    useCreateInviteTokenMutation: () => [vi.fn()],
}));

describe('ViewInviteTokenModal', () => {
    beforeEach(() => {
        getInviteToken.mockClear();
        mockUserContext.platformPrivileges.managePolicies = false;
    });

    it('hides the role picker without Manage Policies and requests a role-less link', () => {
        render(<ViewInviteTokenModal open onClose={() => undefined} />);

        expect(screen.queryByTestId('role-picker')).not.toBeInTheDocument();
        expect(screen.getByText(/will not be assigned a role/)).toBeInTheDocument();
        expect(getInviteToken).toHaveBeenCalledWith(
            expect.objectContaining({ variables: { input: { roleUrn: undefined } } }),
        );
    });

    it('shows the role picker when the caller can manage policies', () => {
        mockUserContext.platformPrivileges.managePolicies = true;
        render(<ViewInviteTokenModal open onClose={() => undefined} />);

        expect(screen.getByTestId('role-picker')).toBeInTheDocument();
        expect(screen.getByText(/automatically assigned to the selected role/)).toBeInTheDocument();
    });
});
