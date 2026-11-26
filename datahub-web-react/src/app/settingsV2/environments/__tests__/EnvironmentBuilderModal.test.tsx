import { toast } from '@components';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { EnvironmentBuilderModal } from '@app/settingsV2/environments/EnvironmentBuilderModal';
import themes from '@conf/theme/themes';

import { CustomFabricType, EntityType } from '@types';

const { createCustomFabricType, updateCustomFabricType } = vi.hoisted(() => ({
    createCustomFabricType: vi.fn(),
    updateCustomFabricType: vi.fn(),
}));

vi.mock('@graphql/customFabricType.generated', () => ({
    useCreateCustomFabricTypeMutation: () => [createCustomFabricType],
    useUpdateCustomFabricTypeMutation: () => [updateCustomFabricType],
}));

const environment: CustomFabricType = {
    urn: 'urn:li:customFabricType:PERF',
    type: EntityType.CustomFabricType,
    id: 'PERF',
    properties: { displayName: 'Performance', description: 'Load tests' },
};

const renderModal = (props: Partial<React.ComponentProps<typeof EnvironmentBuilderModal>> = {}) => {
    const onClose = vi.fn();
    const refetch = vi.fn();
    render(
        <ThemeProvider theme={themes.themeV2}>
            <EnvironmentBuilderModal isOpen onClose={onClose} refetch={refetch} {...props} />
        </ThemeProvider>,
    );
    return { onClose, refetch };
};

describe('EnvironmentBuilderModal', () => {
    beforeEach(() => {
        createCustomFabricType.mockReset();
        updateCustomFabricType.mockReset();
        createCustomFabricType.mockResolvedValue({});
        updateCustomFabricType.mockResolvedValue({});
        vi.spyOn(toast, 'success').mockImplementation(() => 'id');
        vi.spyOn(toast, 'error').mockImplementation(() => 'id');
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('keeps save disabled until the id is a valid environment name', () => {
        renderModal();

        const save = screen.getByTestId('environment-builder-save');
        expect(save).toBeDisabled();

        fireEvent.change(screen.getByTestId('environment-id-input'), { target: { value: 'perf-test' } });
        expect(save).toBeDisabled();

        fireEvent.change(screen.getByTestId('environment-id-input'), { target: { value: ' perf_test ' } });
        expect(save).toBeEnabled();
    });

    it('creates with the normalized id and omits empty optional fields', async () => {
        const { onClose, refetch } = renderModal();

        fireEvent.change(screen.getByTestId('environment-id-input'), { target: { value: ' perf_test ' } });
        fireEvent.click(screen.getByTestId('environment-builder-save'));

        await waitFor(() => expect(createCustomFabricType).toHaveBeenCalledTimes(1));
        const input = createCustomFabricType.mock.calls[0][0].variables.input;
        expect(input.id).toBe('PERF_TEST');
        expect(input.displayName).toBeUndefined();
        expect(input.description).toBeUndefined();
        expect(updateCustomFabricType).not.toHaveBeenCalled();
        await waitFor(() => expect(toast.success).toHaveBeenCalledWith('Created environment PERF_TEST.'));
        expect(onClose).toHaveBeenCalled();
        expect(refetch).toHaveBeenCalled();
    });

    it('sends display name and description, including an empty description, when editing', async () => {
        renderModal({ environment });

        const id = screen.getByTestId('environment-id-input');
        expect(id).toBeDisabled();
        expect(id).toHaveValue('PERF');
        expect(screen.getByTestId('environment-builder-save')).toBeEnabled();

        fireEvent.change(screen.getByTestId('environment-display-name-input'), { target: { value: 'Perf tests' } });
        fireEvent.change(screen.getByTestId('environment-description-input'), { target: { value: '' } });
        fireEvent.click(screen.getByTestId('environment-builder-save'));

        await waitFor(() =>
            expect(updateCustomFabricType).toHaveBeenCalledWith({
                variables: {
                    input: {
                        urn: 'urn:li:customFabricType:PERF',
                        displayName: 'Perf tests',
                        description: '',
                    },
                },
            }),
        );
        expect(createCustomFabricType).not.toHaveBeenCalled();
    });

    it('reports a create failure', async () => {
        createCustomFabricType.mockRejectedValue(new Error('id already exists'));
        renderModal();

        fireEvent.change(screen.getByTestId('environment-id-input'), { target: { value: 'PERF' } });
        fireEvent.click(screen.getByTestId('environment-builder-save'));

        await waitFor(() =>
            expect(toast.error).toHaveBeenCalledWith('Failed to create environment: id already exists'),
        );
        expect(toast.success).not.toHaveBeenCalled();
    });
});
