import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import DeleteLogicalModelColumnButton from '@app/entityV2/shared/logicalModels/DeleteLogicalModelColumnButton';
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
    childCount: 0,
    open: true,
    onClose: vi.fn(),
};

function renderButton() {
    render(
        <ThemeProvider theme={themeV2}>
            <DeleteLogicalModelColumnButton {...defaultProps} />
        </ThemeProvider>,
    );
}

describe('DeleteLogicalModelColumnButton', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        updateSchema.mockResolvedValue({ errors: undefined });
    });

    it('removes only the target column from the existing schema', async () => {
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
        renderButton();
        fireEvent.click(screen.getByTestId('modal-confirm-button'));

        await waitFor(() => expect(updateSchema).toHaveBeenCalledTimes(1));
        expect(updateSchema).toHaveBeenCalledWith({
            variables: {
                input: {
                    urn: URN,
                    columns: [{ fieldPath: 'other', type: SchemaFieldDataType.Number }],
                },
            },
        });
    });

    // Regression guard: a failed/stale read must abort, never write a truncated schema.
    it('does not overwrite the schema when the current columns cannot be read', async () => {
        fetchColumns.mockResolvedValue({ data: null });
        renderButton();
        fireEvent.click(screen.getByTestId('modal-confirm-button'));

        await waitFor(() => expect(fetchColumns).toHaveBeenCalled());
        expect(updateSchema).not.toHaveBeenCalled();
    });
});
