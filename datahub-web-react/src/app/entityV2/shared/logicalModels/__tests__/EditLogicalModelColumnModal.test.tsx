import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import EditLogicalModelColumnModal from '@app/entityV2/shared/logicalModels/EditLogicalModelColumnModal';
import themeV2 from '@conf/theme/themeV2';

import { SchemaFieldDataType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:logical,m,PROD)';

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

const defaultProps = {
    datasetUrn: URN,
    fieldPath: 'my_column',
    currentType: SchemaFieldDataType.String,
    onClose: vi.fn(),
};

function renderModal(childCount: number) {
    return render(
        <ThemeProvider theme={themeV2}>
            <EditLogicalModelColumnModal {...defaultProps} childCount={childCount} />
        </ThemeProvider>,
    );
}

describe('EditLogicalModelColumnModal', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        updateSchema.mockResolvedValue({ errors: undefined });
    });

    it('shows the unlink warning when children exist', () => {
        renderModal(2);
        // i18n is not resolved in tests — the key is returned as-is
        expect(screen.getByText(/editColumn\.unlinkWarning/i)).toBeInTheDocument();
    });

    it('hides the unlink warning when there are no children', () => {
        renderModal(0);
        expect(screen.queryByText(/editColumn\.unlinkWarning/i)).not.toBeInTheDocument();
    });

    it('renames the column within the existing schema rather than replacing it', async () => {
        fetchColumns.mockResolvedValue({
            data: {
                dataset: {
                    schemaMetadata: {
                        fields: [
                            { fieldPath: 'my_column', type: SchemaFieldDataType.String },
                            { fieldPath: 'other', type: SchemaFieldDataType.Number },
                        ],
                    },
                },
            },
        });
        renderModal(0);
        fireEvent.change(screen.getAllByRole('textbox')[0], { target: { value: 'renamed' } });
        fireEvent.click(screen.getByTestId('submit-edit-logical-model-column'));

        await waitFor(() => expect(updateSchema).toHaveBeenCalledTimes(1));
        expect(updateSchema).toHaveBeenCalledWith({
            variables: {
                input: {
                    urn: URN,
                    columns: [
                        { fieldPath: 'renamed', type: SchemaFieldDataType.String },
                        { fieldPath: 'other', type: SchemaFieldDataType.Number },
                    ],
                },
            },
        });
    });

    // Regression guard: a failed/stale read must abort, never write a truncated schema.
    it('does not overwrite the schema when the current columns cannot be read', async () => {
        fetchColumns.mockResolvedValue({ data: null });
        renderModal(0);
        fireEvent.change(screen.getAllByRole('textbox')[0], { target: { value: 'renamed' } });
        fireEvent.click(screen.getByTestId('submit-edit-logical-model-column'));

        await waitFor(() => expect(fetchColumns).toHaveBeenCalled());
        expect(updateSchema).not.toHaveBeenCalled();
    });

    it('does not rename a column onto a name that already exists', async () => {
        fetchColumns.mockResolvedValue({
            data: {
                dataset: {
                    schemaMetadata: {
                        fields: [
                            { fieldPath: 'my_column', type: SchemaFieldDataType.String },
                            { fieldPath: 'other', type: SchemaFieldDataType.Number },
                        ],
                    },
                },
            },
        });
        renderModal(0);
        fireEvent.change(screen.getAllByRole('textbox')[0], { target: { value: 'other' } });
        fireEvent.click(screen.getByTestId('submit-edit-logical-model-column'));

        await waitFor(() => expect(fetchColumns).toHaveBeenCalled());
        expect(updateSchema).not.toHaveBeenCalled();
    });
});
