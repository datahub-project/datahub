import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { AvatarType } from '@components/components/AvatarStack/types';

import AvatarStackWithHover from '@app/sharedV2/avatar/AvatarStackWithHover';
import TestPageContainer, { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

vi.mock('@app/identity/user/UserUtils', () => ({
    mapRoleIcon: (roleName: string) => <span data-testid={`mapped-role-icon-${roleName}`} />,
}));

const renderStack = () =>
    render(
        <MockedProvider>
            <TestPageContainer>
                <AvatarStackWithHover
                    entityRegistry={getTestEntityRegistry()}
                    avatars={[
                        { name: 'Admin', type: AvatarType.role },
                        { name: 'Custom', type: AvatarType.role, icon: <span data-testid="explicit-role-icon" /> },
                        { name: 'Jane Doe', type: AvatarType.user },
                    ]}
                />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('AvatarStackWithHover', () => {
    it('passes mapped role icons to the stack without overriding explicit ones', () => {
        renderStack();

        expect(screen.getByTestId('mapped-role-icon-Admin')).toBeInTheDocument();
        expect(screen.getByTestId('explicit-role-icon')).toBeInTheDocument();
        expect(screen.queryByTestId('mapped-role-icon-Custom')).not.toBeInTheDocument();
        expect(screen.queryByTestId('mapped-role-icon-Jane Doe')).not.toBeInTheDocument();
    });

    it('passes mapped role icons to the hover content', async () => {
        renderStack();

        fireEvent.mouseEnter(screen.getByTestId('mapped-role-icon-Admin'));

        await waitFor(() => {
            expect(screen.getAllByTestId('mapped-role-icon-Admin')).toHaveLength(2);
        });
        expect(screen.getAllByTestId('explicit-role-icon')).toHaveLength(2);
    });
});
