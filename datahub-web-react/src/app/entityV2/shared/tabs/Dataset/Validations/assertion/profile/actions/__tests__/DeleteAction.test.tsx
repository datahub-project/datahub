import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Modal, message } from 'antd';
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
        const confirm = vi.spyOn(Modal, 'confirm').mockImplementation((config) => {
            config.onOk?.();
            return { destroy: vi.fn(), update: vi.fn() };
        });
        vi.spyOn(message, 'success').mockResolvedValue(undefined);
        mockDeleteAssertion.mockResolvedValue({ data: { deleteAssertion: true } });

        render(
            <CustomThemeProvider>
                <DeleteAction assertion={assertion} canEdit refetch={refetch} isExpandedView />
            </CustomThemeProvider>,
        );

        await userEvent.click(screen.getByText('Delete'));

        expect(confirm).toHaveBeenCalled();
        expect(mockDeleteAssertion).toHaveBeenCalledWith({ variables: { urn: assertion.urn } });
        expect(refetch).toHaveBeenCalled();
    });

    it('does not open confirmation without permission', async () => {
        const confirm = vi.spyOn(Modal, 'confirm').mockImplementation(() => ({
            destroy: vi.fn(),
            update: vi.fn(),
        }));

        render(
            <CustomThemeProvider>
                <DeleteAction assertion={assertion} canEdit={false} isExpandedView />
            </CustomThemeProvider>,
        );

        await userEvent.click(screen.getByText('Delete'));

        expect(confirm).not.toHaveBeenCalled();
        expect(mockDeleteAssertion).not.toHaveBeenCalled();
    });
});
