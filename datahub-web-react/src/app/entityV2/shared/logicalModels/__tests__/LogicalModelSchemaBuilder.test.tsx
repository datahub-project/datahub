import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import LogicalModelSchemaBuilder from '@app/entityV2/shared/logicalModels/LogicalModelSchemaBuilder';
import themeV2 from '@conf/theme/themeV2';

import { SchemaFieldDataType } from '@types';

describe('LogicalModelSchemaBuilder', () => {
    it('adds a column row and reports changes', () => {
        const onChange = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <LogicalModelSchemaBuilder
                    columns={[{ fieldPath: '', type: SchemaFieldDataType.String }]}
                    onChange={onChange}
                />
            </ThemeProvider>,
        );
        fireEvent.click(screen.getByTestId('add-logical-column'));
        expect(onChange).toHaveBeenCalled();
        const lastCall = onChange.mock.calls[onChange.mock.calls.length - 1][0];
        expect(lastCall.length).toBe(2);
    });

    it('removes a column row', () => {
        const onChange = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <LogicalModelSchemaBuilder
                    columns={[
                        { fieldPath: 'a', type: SchemaFieldDataType.String },
                        { fieldPath: 'b', type: SchemaFieldDataType.Number },
                    ]}
                    onChange={onChange}
                />
            </ThemeProvider>,
        );
        fireEvent.click(screen.getAllByTestId('remove-logical-column')[0]);
        const lastCall = onChange.mock.calls[onChange.mock.calls.length - 1][0];
        expect(lastCall.length).toBe(1);
        expect(lastCall[0].fieldPath).toBe('b');
    });
});
