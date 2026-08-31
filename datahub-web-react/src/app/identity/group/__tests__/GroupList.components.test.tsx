import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { ListGroupsGroup } from '@app/identity/group/GroupList';
import {
    GroupActionsMenu,
    GroupDescriptionCell,
    GroupMembersCell,
    GroupNameCell,
    GroupRoleCell,
} from '@app/identity/group/GroupList.components';
import { toast } from '@src/alchemy-components';
import theme from '@src/alchemy-components/theme';

import { EntityType, OriginType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: (_type: EntityType, urn: string) => `/group/${urn}`,
        getDisplayName: (_type: EntityType, entity: any) =>
            entity?.editableProperties?.name || entity?.name || entity?.urn,
    }),
}));

vi.mock('@app/analytics', () => ({
    default: { event: vi.fn() },
    EventType: { DeleteEntityEvent: 'DeleteEntityEvent' },
}));

const mockRemoveGroupMutation = vi.fn();
vi.mock('@graphql/group.generated', () => ({
    useRemoveGroupMutation: () => [mockRemoveGroupMutation],
}));

vi.mock('@app/identity/user/SimpleSelectRole', () => ({
    default: ({ onRoleSelect, selectedRole, placeholder }: any) => (
        <div data-testid="select-role">
            <span>{selectedRole?.name || placeholder}</span>
            <button
                type="button"
                data-testid="select-admin"
                onClick={() => onRoleSelect({ urn: 'urn:li:dataHubRole:Admin', name: 'Admin' })}
            >
                Select Admin
            </button>
            <button type="button" data-testid="select-no-role" onClick={() => onRoleSelect(null)}>
                Remove Role
            </button>
        </div>
    ),
}));

vi.mock('@src/alchemy-components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@src/alchemy-components')>();
    return {
        ...actual,
        toast: { success: vi.fn(), error: vi.fn() },
    };
});

function wrap(ui: React.ReactElement) {
    return render(
        <ThemeProvider theme={theme as any}>
            <MemoryRouter>{ui}</MemoryRouter>
        </ThemeProvider>,
    );
}

function makeGroup(overrides: Partial<ListGroupsGroup> = {}): ListGroupsGroup {
    return {
        __typename: 'CorpGroup' as const,
        urn: 'urn:li:corpGroup:engineering',
        type: EntityType.CorpGroup,
        name: 'engineering',
        origin: null,
        info: null,
        editableProperties: {
            __typename: 'CorpGroupEditableProperties' as const,
            description: 'The engineering team',
            slack: null,
            email: null,
            pictureLink: null,
            name: null,
        },
        memberCount: { __typename: 'EntityRelationshipsResult' as const, total: 5 },
        roles: {
            __typename: 'EntityRelationshipsResult' as const,
            relationships: [],
        },
        ...overrides,
    } as ListGroupsGroup;
}

describe('GroupNameCell', () => {
    it('should render the group display name as a link', () => {
        const group = makeGroup();
        wrap(<GroupNameCell group={group} />);

        expect(screen.getByText('engineering')).toBeInTheDocument();
        const link = screen.getByRole('link');
        expect(link).toHaveAttribute('href', `/group/${group.urn}`);
    });

    it('should show External badge for external groups', () => {
        const group = makeGroup({
            origin: { __typename: 'Origin' as any, type: OriginType.External, externalType: null },
        });
        wrap(<GroupNameCell group={group} />);

        expect(screen.getByText('External')).toBeInTheDocument();
    });

    it('should not show External badge for non-external groups', () => {
        const group = makeGroup();
        wrap(<GroupNameCell group={group} />);

        expect(screen.queryByText('External')).not.toBeInTheDocument();
    });
});

describe('GroupDescriptionCell', () => {
    it('should render description from editableProperties', () => {
        const group = makeGroup();
        wrap(<GroupDescriptionCell group={group} />);

        expect(screen.getByText('The engineering team')).toBeInTheDocument();
    });

    it('should fall back to info.description', () => {
        const group = makeGroup({
            editableProperties: null,
            info: {
                __typename: 'CorpGroupInfo' as const,
                description: 'From info',
                displayName: null,
                admins: null,
                members: null,
                groups: null,
                email: null,
                slack: null,
                created: null,
            } as any,
        });
        wrap(<GroupDescriptionCell group={group} />);

        expect(screen.getByText('From info')).toBeInTheDocument();
    });

    it('should render nothing when no description exists', () => {
        const group = makeGroup({ editableProperties: null, info: null });
        const { container } = wrap(<GroupDescriptionCell group={group} />);

        expect(container.firstChild).toBeNull();
    });
});

describe('GroupMembersCell', () => {
    it('should render member count', () => {
        const group = makeGroup({ memberCount: { __typename: 'EntityRelationshipsResult' as const, total: 42 } });
        wrap(<GroupMembersCell group={group} />);

        expect(screen.getByText('42 members')).toBeInTheDocument();
    });

    it('should show 0 members when memberCount is null', () => {
        const group = makeGroup({ memberCount: null } as any);
        wrap(<GroupMembersCell group={group} />);

        expect(screen.getByText('0 members')).toBeInTheDocument();
    });
});

describe('GroupRoleCell', () => {
    const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';
    const selectRoleOptions = [
        { urn: 'urn:li:dataHubRole:Admin', name: 'Admin', type: EntityType.DatahubRole },
        { urn: 'urn:li:dataHubRole:Editor', name: 'Editor', type: EntityType.DatahubRole },
    ] as any;

    it('should render the role selector with current role', () => {
        const group = makeGroup({
            roles: {
                __typename: 'EntityRelationshipsResult' as const,
                relationships: [
                    {
                        __typename: 'EntityRelationship' as const,
                        entity: { urn: 'urn:li:dataHubRole:Admin', name: 'Admin', type: EntityType.DatahubRole },
                    },
                ],
            },
        });
        wrap(
            <GroupRoleCell
                group={group}
                selectRoleOptions={selectRoleOptions}
                noRoleUrn={NO_ROLE_URN}
                onRoleChange={vi.fn()}
            />,
        );

        expect(screen.getByText('Admin')).toBeInTheDocument();
    });

    it('should show placeholder when no role is assigned', () => {
        const group = makeGroup();
        wrap(
            <GroupRoleCell
                group={group}
                selectRoleOptions={selectRoleOptions}
                noRoleUrn={NO_ROLE_URN}
                onRoleChange={vi.fn()}
            />,
        );

        expect(screen.getByText('No Role')).toBeInTheDocument();
    });

    it('should use optimistic role over server role', () => {
        const group = makeGroup();
        wrap(
            <GroupRoleCell
                group={group}
                selectRoleOptions={selectRoleOptions}
                optimisticRoleUrn="urn:li:dataHubRole:Editor"
                noRoleUrn={NO_ROLE_URN}
                onRoleChange={vi.fn()}
            />,
        );

        expect(screen.getByText('Editor')).toBeInTheDocument();
    });

    it('should call onRoleChange when a different role is selected', () => {
        const onRoleChange = vi.fn();
        const group = makeGroup();
        wrap(
            <GroupRoleCell
                group={group}
                selectRoleOptions={selectRoleOptions}
                noRoleUrn={NO_ROLE_URN}
                onRoleChange={onRoleChange}
            />,
        );

        fireEvent.click(screen.getByTestId('select-admin'));

        expect(onRoleChange).toHaveBeenCalledWith(group.urn, 'engineering', 'urn:li:dataHubRole:Admin', NO_ROLE_URN);
    });

    it('should not call onRoleChange when same role is selected', () => {
        const onRoleChange = vi.fn();
        const group = makeGroup({
            roles: {
                __typename: 'EntityRelationshipsResult' as const,
                relationships: [
                    {
                        __typename: 'EntityRelationship' as const,
                        entity: { urn: 'urn:li:dataHubRole:Admin', name: 'Admin', type: EntityType.DatahubRole },
                    },
                ],
            },
        });
        wrap(
            <GroupRoleCell
                group={group}
                selectRoleOptions={selectRoleOptions}
                noRoleUrn={NO_ROLE_URN}
                onRoleChange={onRoleChange}
            />,
        );

        fireEvent.click(screen.getByTestId('select-admin'));

        expect(onRoleChange).not.toHaveBeenCalled();
    });
});

describe('GroupActionsMenu', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        Object.assign(navigator, {
            clipboard: { writeText: vi.fn().mockResolvedValue(undefined) },
        });
    });

    it('should render the menu trigger button', () => {
        const group = makeGroup();
        wrap(<GroupActionsMenu group={group} onDelete={vi.fn()} />);

        expect(screen.getByTestId('group-menu-engineering')).toBeInTheDocument();
    });

    it('should call onDelete after successful deletion', async () => {
        mockRemoveGroupMutation.mockResolvedValue({ errors: undefined });
        const onDelete = vi.fn();
        const group = makeGroup();
        wrap(<GroupActionsMenu group={group} onDelete={onDelete} />);

        fireEvent.click(screen.getByTestId('group-menu-engineering'));
        await waitFor(() => {
            expect(screen.getByText('Delete')).toBeInTheDocument();
        });
        fireEvent.click(screen.getByText('Delete'));

        await waitFor(() => {
            expect(screen.getByText(/Are you sure you want to delete/)).toBeInTheDocument();
        });

        const confirmDeleteButtons = screen.getAllByText('Delete');
        const confirmButton = confirmDeleteButtons.find(
            (btn) => btn.closest('.ant-modal, [class*="modal"]') || btn.closest('button[class*="red"]'),
        );
        fireEvent.click(confirmButton || confirmDeleteButtons[confirmDeleteButtons.length - 1]);

        await waitFor(() => {
            expect(mockRemoveGroupMutation).toHaveBeenCalledWith({ variables: { urn: group.urn } });
        });
        await waitFor(() => {
            expect(onDelete).toHaveBeenCalledWith(group.urn);
            expect(toast.success).toHaveBeenCalledWith('Deleted engineering!');
        });
    });

    it('should show error toast on deletion failure', async () => {
        mockRemoveGroupMutation.mockRejectedValue(new Error('Server error'));
        const group = makeGroup();
        wrap(<GroupActionsMenu group={group} onDelete={vi.fn()} />);

        fireEvent.click(screen.getByTestId('group-menu-engineering'));
        await waitFor(() => {
            expect(screen.getByText('Delete')).toBeInTheDocument();
        });
        fireEvent.click(screen.getByText('Delete'));

        await waitFor(() => {
            expect(screen.getByText(/Are you sure you want to delete/)).toBeInTheDocument();
        });

        const confirmDeleteButtons = screen.getAllByText('Delete');
        fireEvent.click(confirmDeleteButtons[confirmDeleteButtons.length - 1]);

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Failed to delete: Server error');
        });
    });
});
