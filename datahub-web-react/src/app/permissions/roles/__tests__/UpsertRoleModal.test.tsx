import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import UpsertRoleModal from '@app/permissions/roles/UpsertRoleModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { CreateRoleDocument, UpdateRoleDocument } from '@graphql/mutations.generated';
import { DataHubRole, EntityType } from '@types';

vi.mock('@app/sharedV2/toastMessageUtils', () => ({
    ToastType: { SUCCESS: 'SUCCESS', ERROR: 'ERROR' },
    showToastMessage: vi.fn(),
}));

const CUSTOM_ROLE: DataHubRole = {
    __typename: 'DataHubRole',
    urn: 'urn:li:dataHubRole:custom-uuid',
    type: EntityType.DatahubRole,
    name: 'Data Steward',
    description: 'Owns data quality for a domain',
    editable: true,
};

function renderModal(props: {
    role?: DataHubRole;
    mocks: React.ComponentProps<typeof MockedProvider>['mocks'];
    onSave?: () => void;
    onClose?: () => void;
}) {
    const onSave = props.onSave ?? vi.fn();
    const onClose = props.onClose ?? vi.fn();
    return {
        ...render(
            <MockedProvider mocks={props.mocks} addTypename={false}>
                <TestPageContainer>
                    <UpsertRoleModal open role={props.role} onClose={onClose} onSave={onSave} />
                </TestPageContainer>
            </MockedProvider>,
        ),
        onSave,
        onClose,
    };
}

describe('UpsertRoleModal', () => {
    it('renders no modal content when open is false', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <UpsertRoleModal open={false} onClose={vi.fn()} onSave={vi.fn()} />
                </TestPageContainer>
            </MockedProvider>,
        );
        // The component returns null when not open; no Create/Save UI should be reachable.
        expect(screen.queryByText('Create Role')).not.toBeInTheDocument();
        expect(screen.queryByText('Save')).not.toBeInTheDocument();
    });

    it('calls createRole mutation when role prop is absent', async () => {
        const createResult = vi.fn(() => ({
            data: { createRole: 'urn:li:dataHubRole:generated' },
        }));

        const { onSave } = renderModal({
            mocks: [
                {
                    request: {
                        query: CreateRoleDocument,
                        variables: { input: { name: 'New Role', description: 'New desc' } },
                    },
                    newData: createResult,
                },
            ],
        });

        // Title and primary button reflect create mode (vs. edit mode).
        expect(screen.getByText('Create Role')).toBeInTheDocument();
        expect(screen.getByText('Create')).toBeInTheDocument();

        const nameInput = screen.getByPlaceholderText('e.g. Data Steward');
        fireEvent.change(nameInput, { target: { value: 'New Role' } });

        const descInput = screen.getByPlaceholderText('Describe the purpose of this role...');
        fireEvent.change(descInput, { target: { value: 'New desc' } });

        fireEvent.click(screen.getByText('Create'));

        await waitFor(() => expect(createResult).toHaveBeenCalledTimes(1));
        await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    });

    it('pre-fills form and routes to updateRole mutation when role prop is provided', async () => {
        const updateResult = vi.fn(() => ({ data: { updateRole: true } }));

        const { onSave } = renderModal({
            role: CUSTOM_ROLE,
            mocks: [
                {
                    request: {
                        query: UpdateRoleDocument,
                        variables: {
                            input: {
                                urn: CUSTOM_ROLE.urn,
                                name: 'Data Steward (renamed)',
                                description: CUSTOM_ROLE.description ?? '',
                            },
                        },
                    },
                    newData: updateResult,
                },
            ],
        });

        // Edit-mode title carries the existing role name; pre-filled form values are visible.
        expect(screen.getByText(`Edit ${CUSTOM_ROLE.name}`)).toBeInTheDocument();
        expect(screen.getByDisplayValue(CUSTOM_ROLE.name)).toBeInTheDocument();
        expect(screen.getByDisplayValue(CUSTOM_ROLE.description ?? '')).toBeInTheDocument();

        const nameInput = screen.getByDisplayValue(CUSTOM_ROLE.name);
        fireEvent.change(nameInput, { target: { value: 'Data Steward (renamed)' } });

        fireEvent.click(screen.getByText('Save'));

        await waitFor(() => expect(updateResult).toHaveBeenCalledTimes(1));
        await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    });

    it('does not call any mutation when name is empty (required validation)', async () => {
        const createResult = vi.fn(() => ({
            data: { createRole: 'urn:li:dataHubRole:generated' },
        }));

        renderModal({
            mocks: [
                {
                    request: {
                        query: CreateRoleDocument,
                        variables: { input: { name: '', description: undefined } },
                    },
                    newData: createResult,
                },
            ],
        });

        // Submit without filling in the required Name field — antd's form validation should
        // short-circuit before the mutation is invoked. This protects the resolver from a
        // server-side trip on a guaranteed bad payload.
        fireEvent.click(screen.getByText('Create'));

        // Give async validation a tick to settle, then assert no mutation was issued.
        await new Promise((resolve) => {
            setTimeout(resolve, 50);
        });
        expect(createResult).not.toHaveBeenCalled();
    });
});
