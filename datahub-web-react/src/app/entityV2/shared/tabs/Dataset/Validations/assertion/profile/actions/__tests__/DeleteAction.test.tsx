import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DeleteAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/DeleteAction';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { Assertion, EntityType } from '@types';

const mockDeleteAssertion = vi.fn();

vi.mock('@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks', () => ({
    useDeleteAssertionMutationWithCache: () => [mockDeleteAssertion],
}));

describe('DeleteAction', () => {
    const assertion = {
        urn: 'urn:li:assertion:test',
        type: EntityType.Assertion,
    } as Assertion;

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('deletes an editable assertion after confirmation', async () => {
        const refetch = vi.fn();
        mockDeleteAssertion.mockResolvedValue({ data: { deleteAssertion: true } });

        render(
            <CustomThemeProvider>
                <DeleteAction assertion={assertion} canEdit refetch={refetch} isExpandedView />
            </CustomThemeProvider>,
        );

        await userEvent.click(screen.getByText('Delete'));
        await userEvent.click(await screen.findByTestId('modal-confirm-button'));

        expect(mockDeleteAssertion).toHaveBeenCalledWith({ variables: { urn: assertion.urn } });
        expect(refetch).toHaveBeenCalled();
    });

    it('does not open confirmation without permission', async () => {
        render(
            <CustomThemeProvider>
                <DeleteAction assertion={assertion} canEdit={false} isExpandedView />
            </CustomThemeProvider>,
        );

        await userEvent.click(screen.getByText('Delete'));

        expect(screen.queryByTestId('modal-confirm-button')).not.toBeInTheDocument();
        expect(mockDeleteAssertion).not.toHaveBeenCalled();
    });
});
