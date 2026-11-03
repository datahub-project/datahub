import { fireEvent } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import ViewInviteTokenModal from '@app/identity/user/ViewInviteTokenModal';
import { render, screen } from '@utils/test-utils/customRender';

const SELECTED_ROLE_URN = 'urn:li:dataHubRole:Admin';

const mockUserContext = {
    loaded: true,
    platformPrivileges: {
        managePolicies: false,
    },
};

const getInviteToken = vi.fn();

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => mockUserContext,
}));

vi.mock('@app/identity/user/SimpleSelectRole', () => ({
    default: ({ onRoleSelect }: { onRoleSelect: (role: { urn: string; name: string }) => void }) => (
        <button
            type="button"
            aria-label="role"
            data-testid="role-picker"
            onClick={() => onRoleSelect({ urn: 'urn:li:dataHubRole:Admin', name: 'Admin' })}
        />
    ),
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
        mockUserContext.loaded = true;
        mockUserContext.platformPrivileges.managePolicies = false;
    });

    it('waits for privileges before requesting a token or showing the no-role footer', () => {
        mockUserContext.loaded = false;
        render(<ViewInviteTokenModal open onClose={() => undefined} />);

        expect(screen.queryByText(/will not be assigned a role/)).not.toBeInTheDocument();
        expect(screen.queryByTestId('role-picker')).not.toBeInTheDocument();
        expect(getInviteToken).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
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

        fireEvent.click(screen.getByTestId('role-picker'));

        expect(getInviteToken).toHaveBeenLastCalledWith(
            expect.objectContaining({ variables: { input: { roleUrn: SELECTED_ROLE_URN } } }),
        );
    });
});
