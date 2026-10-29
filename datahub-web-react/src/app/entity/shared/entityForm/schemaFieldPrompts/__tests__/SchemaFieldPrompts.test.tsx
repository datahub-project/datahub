import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import SchemaFieldPrompts from '@app/entity/shared/entityForm/schemaFieldPrompts/SchemaFieldPrompts';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { FormPrompt } from '@types';

const { mockUseGetEntityWithSchema } = vi.hoisted(() => ({ mockUseGetEntityWithSchema: vi.fn() }));

vi.mock('@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema', () => ({
    useGetEntityWithSchema: mockUseGetEntityWithSchema,
}));

// The row itself is covered by its own component; here we only care which rows the filter shows.
// The uncontrolled input stands in for an unsaved answer held in the row's own state.
vi.mock('@app/entity/shared/entityForm/schemaFieldPrompts/SchemaFieldDropdown', () => ({
    default: ({ field }: { field: { fieldPath: string } }) => (
        <div>
            <span>row:{field.fieldPath}</span>
            <input aria-label={`answer-${field.fieldPath}`} />
        </div>
    ),
}));

vi.mock('@app/sharedV2/VirtualScrollChild', () => ({
    default: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock('react-i18next', () => ({
    useTranslation: () => ({
        t: (key: string, opts?: Record<string, string>) => (opts?.searchQuery ? `${key}:${opts.searchQuery}` : key),
    }),
}));

const COLUMNS = ['transaction_id', 'customer_id', 'Amount', 'created_at', 'archived_on'];
const PROMPTS = [{ id: 'prompt1' }] as FormPrompt[];

function renderPrompts() {
    render(
        <CustomThemeProvider>
            <SchemaFieldPrompts prompts={PROMPTS} />
        </CustomThemeProvider>,
    );
    const searchBar = screen.getByTestId('field-prompts-search');
    return (searchBar.tagName === 'INPUT' ? searchBar : searchBar.querySelector('input')) as HTMLInputElement;
}

function visibleColumns() {
    return screen
        .queryAllByText(/^row:/)
        .filter((el) => !el.closest('[hidden]'))
        .map((el) => el.textContent?.replace('row:', ''));
}

describe('SchemaFieldPrompts column search', () => {
    beforeEach(() => {
        mockUseGetEntityWithSchema.mockReturnValue({
            entityWithSchema: { schemaMetadata: { fields: COLUMNS.map((fieldPath) => ({ fieldPath })) } },
        });
    });

    it('lists every column before anything is typed', () => {
        renderPrompts();
        expect(visibleColumns()).toEqual(COLUMNS);
    });

    it('narrows the list to the columns that match', () => {
        const input = renderPrompts();
        fireEvent.change(input, { target: { value: 'ed_' } });
        expect(visibleColumns()).toEqual(['created_at', 'archived_on']);
    });

    it('matches regardless of case', () => {
        const input = renderPrompts();
        fireEvent.change(input, { target: { value: 'AMOUNT' } });
        expect(visibleColumns()).toEqual(['Amount']);
    });

    it('ignores surrounding whitespace', () => {
        const input = renderPrompts();
        fireEvent.change(input, { target: { value: '  customer  ' } });
        expect(visibleColumns()).toEqual(['customer_id']);
    });

    it('says so when nothing matches, rather than showing an empty list', () => {
        const input = renderPrompts();
        fireEvent.change(input, { target: { value: 'no_such_column' } });
        expect(visibleColumns()).toEqual([]);
        expect(screen.getByTestId('field-prompts-no-results')).toBeInTheDocument();
    });

    it('keeps an unsaved answer when its row is filtered out and shown again', () => {
        const input = renderPrompts();
        fireEvent.change(screen.getByLabelText('answer-Amount'), { target: { value: 'draft answer' } });
        fireEvent.change(input, { target: { value: 'customer' } });
        expect(visibleColumns()).toEqual(['customer_id']);
        fireEvent.change(input, { target: { value: '' } });
        expect((screen.getByLabelText('answer-Amount') as HTMLInputElement).value).toBe('draft answer');
    });

    it('does not match the type annotations inside a v2 field path', () => {
        mockUseGetEntityWithSchema.mockReturnValue({
            entityWithSchema: {
                schemaMetadata: {
                    fields: [
                        { fieldPath: '[version=2.0].[type=struct].[type=string].name' },
                        { fieldPath: '[version=2.0].[type=struct].[type=long].version_code' },
                    ],
                },
            },
        });
        const input = renderPrompts();
        fireEvent.change(input, { target: { value: 'string' } });
        expect(visibleColumns()).toEqual([]);
        fireEvent.change(input, { target: { value: 'version' } });
        expect(visibleColumns()).toEqual(['[version=2.0].[type=struct].[type=long].version_code']);
    });

    it('renders nothing when the entity has no schema', () => {
        mockUseGetEntityWithSchema.mockReturnValue({ entityWithSchema: { schemaMetadata: { fields: [] } } });
        const { container } = render(
            <CustomThemeProvider>
                <SchemaFieldPrompts prompts={PROMPTS} />
            </CustomThemeProvider>,
        );
        expect(container).toBeEmptyDOMElement();
    });
});
