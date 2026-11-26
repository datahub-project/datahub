import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import SelectColumnReplacementModal from '@app/entityV2/shared/EntityDropdown/SelectColumnReplacementModal';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { EntityType } from '@src/types.generated';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const DBT_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.orders,PROD)';
const SNOWFLAKE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)';
const USERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.users,PROD)';

const dataset = (urn: string, name: string) => ({ urn, type: 'DATASET', name, properties: { name } });
const fields = (...paths: string[]) => ({ fields: paths.map((fieldPath) => ({ fieldPath })) });

// The schema of the dbt side of a sibling pair is carried by the snowflake side, which is the case
// the picker has to cope with: its own schemaMetadata is empty.
const SCHEMAS: Record<string, any> = {
    [DBT_URN]: {
        urn: DBT_URN,
        schemaMetadata: null,
        siblings: {
            isPrimary: true,
            siblings: [{ ...dataset(SNOWFLAKE_URN, 'orders'), schemaMetadata: fields('order_id', 'amount') }],
        },
        siblingsSearch: {
            count: 1,
            total: 1,
            searchResults: [
                { entity: { ...dataset(SNOWFLAKE_URN, 'orders'), schemaMetadata: fields('order_id', 'amount') } },
            ],
        },
    },
    [USERS_URN]: {
        urn: USERS_URN,
        schemaMetadata: fields('user_id', 'email'),
        siblings: null,
        siblingsSearch: { count: 0, total: 0, searchResults: [] },
    },
};

// Partial mocks: these generated modules also export documents that the imported tree pulls in.
vi.mock('@graphql/dataset.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/dataset.generated')>()),
    useGetDatasetSchemaQuery: ({ variables, skip }: { variables?: { urn?: string }; skip?: boolean }) => ({
        data: skip ? undefined : { dataset: SCHEMAS[variables?.urn ?? ''] ?? null },
    }),
}));

vi.mock('@graphql/entity.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/entity.generated')>()),
    useGetEntitiesQuery: ({ variables, skip }: { variables?: { urns?: string[] }; skip?: boolean }) => {
        const urn = variables?.urns?.[0] ?? '';
        if (skip || !urn) return { data: undefined };
        return { data: { entities: [dataset(urn, urn.includes('users') ? 'users' : 'orders')] } };
    },
}));

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetSearchResultsForMultipleQuery: () => ({
        data: {
            searchAcrossEntities: {
                searchResults: [{ entity: dataset(DBT_URN, 'orders') }, { entity: dataset(USERS_URN, 'users') }],
            },
        },
    }),
    useGetAutoCompleteMultipleResultsLazyQuery: () => [vi.fn(), { data: undefined }],
}));

beforeEach(mockVisibilityObserver);

describe('SelectColumnReplacementModal', () => {
    const renderModal = (props: Partial<React.ComponentProps<typeof SelectColumnReplacementModal>> = {}) => {
        const onSave = vi.fn();
        const onCancel = vi.fn();
        render(
            <CustomThemeProvider>
                <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                    <MemoryRouter>
                        <SelectColumnReplacementModal
                            parentEntityType={EntityType.Dataset}
                            initialTableUrn={DBT_URN}
                            onSave={onSave}
                            onCancel={onCancel}
                            {...props}
                        />
                    </MemoryRouter>
                </EntityRegistryContext.Provider>
            </CustomThemeProvider>,
        );
        return { onSave, onCancel };
    };

    const openColumns = () => fireEvent.click(screen.getByTestId('deprecation-replacement-column-base'));
    const openParents = () => fireEvent.click(screen.getByTestId('entity-search-input-v2'));
    // The closed select keeps showing its selection, so the option carrying the same name is the
    // last match in the document — the dropdown renders into a portal after it.
    const clickParentOption = (name: string) => {
        const matches = screen.getAllByText(name);
        fireEvent.click(matches[matches.length - 1]);
    };

    it('offers the columns a sibling carries for the selected parent', () => {
        renderModal();

        openColumns();

        expect(screen.getByTestId('option-order_id')).toBeInTheDocument();
        expect(screen.getByTestId('option-amount')).toBeInTheDocument();
    });

    // The urn names whoever declares the column, so it points at a schemaField that exists.
    it('saves a sibling-carried column under the sibling that declares it', () => {
        const { onSave } = renderModal();

        openColumns();
        fireEvent.click(screen.getByTestId('option-amount'));
        fireEvent.click(screen.getByTestId('select-replacement-save'));

        expect(onSave).toHaveBeenCalledWith(`urn:li:schemaField:(${SNOWFLAKE_URN},amount)`);
    });

    it('saves a column under the selected parent when it declares it itself', () => {
        const { onSave } = renderModal({ initialTableUrn: USERS_URN });

        openColumns();
        fireEvent.click(screen.getByTestId('option-email'));
        fireEvent.click(screen.getByTestId('select-replacement-save'));

        expect(onSave).toHaveBeenCalledWith(`urn:li:schemaField:(${USERS_URN},email)`);
    });

    it('drops the chosen column when the parent changes', () => {
        renderModal();

        openColumns();
        fireEvent.click(screen.getByTestId('option-amount'));

        // Switching the parent to an unrelated table: its columns are the only ones on offer.
        openParents();
        clickParentOption('users');
        openColumns();
        expect(screen.getByTestId('option-user_id')).toBeInTheDocument();
        expect(screen.queryByTestId('option-amount')).not.toBeInTheDocument();

        // Nothing left to save: the parent alone is not a replacement.
        expect(screen.getByTestId('select-replacement-save')).toBeDisabled();
    });

    it('cannot be saved until a column is chosen', () => {
        renderModal();

        expect(screen.getByTestId('select-replacement-save')).toBeDisabled();

        openColumns();
        fireEvent.click(screen.getByTestId('option-amount'));

        expect(screen.getByTestId('select-replacement-save')).toBeEnabled();
    });

    it('keeps the chosen column when the same parent is picked again', () => {
        const { onSave } = renderModal();

        openColumns();
        fireEvent.click(screen.getByTestId('option-amount'));
        openParents();
        clickParentOption('orders');
        fireEvent.click(screen.getByTestId('select-replacement-save'));

        expect(onSave).toHaveBeenCalledWith(`urn:li:schemaField:(${SNOWFLAKE_URN},amount)`);
    });

    it('leaves an existing replacement alone when cancelled', () => {
        const { onSave, onCancel } = renderModal();

        openColumns();
        fireEvent.click(screen.getByTestId('option-amount'));
        fireEvent.click(screen.getByTestId('select-replacement-cancel'));

        expect(onCancel).toHaveBeenCalled();
        expect(onSave).not.toHaveBeenCalled();
    });
});
