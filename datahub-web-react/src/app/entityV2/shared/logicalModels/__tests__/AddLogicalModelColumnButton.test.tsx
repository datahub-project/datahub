import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import AddLogicalModelColumnButton from '@app/entityV2/shared/logicalModels/AddLogicalModelColumnButton';
import themeV2 from '@conf/theme/themeV2';

import { SchemaFieldDataType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:logical,m,PROD)';

const refetch = vi.fn();
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ urn: URN }),
    useRefetch: () => refetch,
}));

const { fetchColumns, updateSchema } = vi.hoisted(() => ({
    fetchColumns: vi.fn(),
    updateSchema: vi.fn(),
}));

vi.mock('@graphql/logical.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/logical.generated')>();
    return {
        ...actual,
        useGetLogicalModelColumnsLazyQuery: () => [fetchColumns],
        useUpdateLogicalModelSchemaMutation: () => [updateSchema],
    };
});

function renderButton() {
    render(
        <ThemeProvider theme={themeV2}>
            <AddLogicalModelColumnButton />
        </ThemeProvider>,
    );
}

async function openModalAndEnterName(name: string) {
    fireEvent.click(screen.getByTestId('add-logical-model-column-button'));
    await waitFor(() => expect(screen.getByTestId('submit-add-logical-model-column')).toBeInTheDocument());
    // The Name input is the first textbox in the modal.
    fireEvent.change(screen.getAllByRole('textbox')[0], { target: { value: name } });
}

describe('AddLogicalModelColumnButton', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        updateSchema.mockResolvedValue({ errors: undefined });
    });

    it('opens the add-column modal on click', async () => {
        renderButton();
        fireEvent.click(screen.getByTestId('add-logical-model-column-button'));
        await waitFor(() => expect(screen.getByTestId('submit-add-logical-model-column')).toBeInTheDocument());
    });

    it('appends the new column to the existing schema rather than replacing it', async () => {
        fetchColumns.mockResolvedValue({
            data: {
                dataset: {
                    schemaMetadata: {
                        fields: [
                            { fieldPath: 'id', type: SchemaFieldDataType.Number },
                            { fieldPath: 'name', type: SchemaFieldDataType.String },
                        ],
                    },
                },
            },
        });
        renderButton();
        await openModalAndEnterName('email');
        fireEvent.click(screen.getByTestId('submit-add-logical-model-column'));

        await waitFor(() => expect(updateSchema).toHaveBeenCalledTimes(1));
        expect(updateSchema).toHaveBeenCalledWith({
            variables: {
                input: {
                    urn: URN,
                    columns: [
                        { fieldPath: 'id', type: SchemaFieldDataType.Number },
                        { fieldPath: 'name', type: SchemaFieldDataType.String },
                        { fieldPath: 'email', type: SchemaFieldDataType.String },
                    ],
                },
            },
        });
    });

    // Regression guard: a failed/stale read must abort, never write a single-column schema that
    // would wipe the rest of the columns and unlink every physical child.
    it('does not overwrite the schema when the current columns cannot be read', async () => {
        fetchColumns.mockResolvedValue({ data: null });
        renderButton();
        await openModalAndEnterName('email');
        fireEvent.click(screen.getByTestId('submit-add-logical-model-column'));

        await waitFor(() => expect(fetchColumns).toHaveBeenCalled());
        expect(updateSchema).not.toHaveBeenCalled();
    });

    it('does not add a column whose name already exists', async () => {
        fetchColumns.mockResolvedValue({
            data: {
                dataset: {
                    schemaMetadata: { fields: [{ fieldPath: 'id', type: SchemaFieldDataType.Number }] },
                },
            },
        });
        renderButton();
        await openModalAndEnterName('id');
        fireEvent.click(screen.getByTestId('submit-add-logical-model-column'));

        await waitFor(() => expect(fetchColumns).toHaveBeenCalled());
        expect(updateSchema).not.toHaveBeenCalled();
    });
});
