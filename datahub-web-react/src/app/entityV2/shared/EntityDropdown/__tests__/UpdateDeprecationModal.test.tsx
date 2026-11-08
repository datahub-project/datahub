import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { UpdateDeprecationModal } from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { EntityType, SubResourceType } from '@src/types.generated';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)';
const USERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.users,PROD)';
const FIELD_PATH = 'user_id';

const dataset = (urn: string, name: string) => ({ urn, type: 'DATASET', name, properties: { name } });
const named = (urn: string) => dataset(urn, urn.includes('users') ? 'users' : 'orders');
const fields = (...paths: string[]) => ({ fields: paths.map((fieldPath) => ({ fieldPath })) });

const SCHEMAS: Record<string, any> = {
    [DATASET_URN]: { urn: DATASET_URN, schemaMetadata: fields('user_id', 'amount') },
    [USERS_URN]: { urn: USERS_URN, schemaMetadata: fields('email') },
};

const batchUpdateDeprecation = vi.fn();

vi.mock('@graphql/mutations.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/mutations.generated')>()),
    useBatchUpdateDeprecationMutation: () => [batchUpdateDeprecation],
}));

vi.mock('@graphql/entity.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/entity.generated')>()),
    useGetEntitiesQuery: ({ variables, skip }: { variables?: { urns?: string[] }; skip?: boolean }) => {
        const urn = variables?.urns?.[0] ?? '';
        if (skip || !urn) return { data: undefined, loading: false };
        return { data: { entities: [named(urn)] }, loading: false };
    },
}));

vi.mock('@graphql/dataset.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/dataset.generated')>()),
    useGetDatasetSchemaQuery: ({ variables, skip }: { variables?: { urn?: string }; skip?: boolean }) => ({
        data: skip
            ? undefined
            : {
                  dataset: SCHEMAS[variables?.urn ?? '']
                      ? {
                            ...SCHEMAS[variables?.urn ?? ''],
                            siblings: null,
                            siblingsSearch: { count: 0, total: 0, searchResults: [] },
                        }
                      : null,
              },
    }),
}));

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetSearchResultsForMultipleQuery: () => ({
        data: {
            searchAcrossEntities: {
                searchResults: [{ entity: named(DATASET_URN) }, { entity: named(USERS_URN) }],
            },
        },
    }),
    useGetAutoCompleteMultipleResultsLazyQuery: () => [vi.fn(), { data: undefined }],
}));

vi.mock('@components', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@components')>()),
    toast: { loading: vi.fn(), success: vi.fn(), error: vi.fn(), destroy: vi.fn() },
}));

// The imported tree reaches for several analytics methods and event names; every one of them is a
// no-op here, so the mock answers whatever is asked of it.
vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: new Proxy({}, { get: () => vi.fn() }),
    EventType: new Proxy({}, { get: (_target, key) => String(key) }),
}));

beforeEach(() => {
    batchUpdateDeprecation.mockResolvedValue({});
    mockVisibilityObserver();
});

describe('UpdateDeprecationModal', () => {
    const resourceRefs = [
        { resourceUrn: DATASET_URN, subResource: FIELD_PATH, subResourceType: SubResourceType.DatasetField },
    ];

    const renderModal = (props = {}) => {
        const onClose = vi.fn();
        const refetch = vi.fn();
        render(
            <CustomThemeProvider>
                <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                    <MemoryRouter>
                        <UpdateDeprecationModal
                            urns={[DATASET_URN]}
                            resourceRefs={resourceRefs}
                            onClose={onClose}
                            refetch={refetch}
                            {...props}
                        />
                    </MemoryRouter>
                </EntityRegistryContext.Provider>
            </CustomThemeProvider>,
        );
        return { onClose, refetch };
    };

    it('deprecates a column with the replacement picked for it', async () => {
        const { onClose, refetch } = renderModal();

        fireEvent.click(screen.getByTestId('select-replacement'));
        fireEvent.click(screen.getByTestId('deprecation-replacement-column-base'));
        fireEvent.click(screen.getByTestId('option-amount'));
        fireEvent.click(screen.getByTestId('select-replacement-save'));

        // The replacement is named by parent and column, since a field path alone is ambiguous.
        expect(screen.getByTestId('edit-replacement')).toHaveTextContent('orders.amount');

        fireEvent.click(screen.getByTestId('add-deprecation-submit'));

        const replacementUrn = `urn:li:schemaField:(${DATASET_URN},amount)`;
        await waitFor(() =>
            expect(batchUpdateDeprecation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        resources: resourceRefs,
                        deprecated: true,
                        note: '',
                        decommissionTime: null,
                        replacement: replacementUrn,
                    },
                },
            }),
        );
        await waitFor(() =>
            expect(refetch).toHaveBeenCalledWith({
                note: null,
                decommissionTime: null,
                replacement: { urn: replacementUrn, type: EntityType.SchemaField },
            }),
        );
        expect(onClose).toHaveBeenCalled();
    });

    // The point of the feature: the replacement may live in an asset other than the deprecated column's.
    it('deprecates a column with a replacement taken from another parent', async () => {
        renderModal();

        fireEvent.click(screen.getByTestId('select-replacement'));
        fireEvent.click(screen.getByTestId('entity-search-input-v2'));
        // The closed parent select still shows its own selection, so the option is the last match.
        const parents = screen.getAllByText('users');
        fireEvent.click(parents[parents.length - 1]);
        fireEvent.click(screen.getByTestId('deprecation-replacement-column-base'));
        fireEvent.click(screen.getByTestId('option-email'));
        fireEvent.click(screen.getByTestId('select-replacement-save'));

        expect(screen.getByTestId('edit-replacement')).toHaveTextContent('users.email');

        fireEvent.click(screen.getByTestId('add-deprecation-submit'));

        await waitFor(() =>
            expect(batchUpdateDeprecation).toHaveBeenCalledWith(
                expect.objectContaining({
                    variables: expect.objectContaining({
                        input: expect.objectContaining({
                            replacement: `urn:li:schemaField:(${USERS_URN},email)`,
                        }),
                    }),
                }),
            ),
        );
    });

    it('shows the replacement an existing column deprecation already carries', () => {
        renderModal({
            initialDeprecation: {
                deprecated: true,
                note: 'moved',
                decommissionTime: null,
                replacement: {
                    urn: `urn:li:schemaField:(${DATASET_URN},amount)`,
                    type: EntityType.SchemaField,
                },
            },
        });

        expect(screen.getByTestId('edit-replacement')).toHaveTextContent('orders.amount');
        expect(screen.queryByTestId('select-replacement')).not.toBeInTheDocument();
    });
});
