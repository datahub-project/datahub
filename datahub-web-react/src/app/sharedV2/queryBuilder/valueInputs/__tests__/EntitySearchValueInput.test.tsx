/* eslint-disable react/no-unused-prop-types -- mock Select only captures props */
import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EntitySearchValueInput } from '@app/sharedV2/queryBuilder/valueInputs/EntitySearchValueInput';

import { EntityType } from '@types';

type CapturedSelect = {
    filterResultsByQuery?: boolean;
    onSearchChange?: (value: string) => void;
};

const mocks = vi.hoisted(() => ({
    searchResources: vi.fn(),
    selectProps: undefined as CapturedSelect | undefined,
}));

vi.mock('@components', async () => {
    const actual = await vi.importActual<typeof import('@components')>('@components');
    return {
        ...actual,
        // Captures the props the real picker passes through; the mock does not render them.
        Select: (props: CapturedSelect) => {
            mocks.selectProps = props;
            return <div />;
        },
    };
});

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getDisplayName: vi.fn() }),
}));

vi.mock('@graphql/entity.generated', () => ({
    useGetEntitiesLazyQuery: () => [vi.fn(), {}],
}));

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetSearchResultsForMultipleLazyQuery: () => [mocks.searchResources, {}],
}));

describe('EntitySearchValueInput', () => {
    beforeEach(() => {
        mocks.searchResources.mockReset();
        mocks.selectProps = undefined;
    });

    it('requests 20 server results and leaves them unfiltered by title', async () => {
        render(
            <EntitySearchValueInput
                selectedUrns={[]}
                entityTypes={[EntityType.Dataset]}
                onChangeSelectedUrns={vi.fn()}
            />,
        );

        await waitFor(() =>
            expect(mocks.searchResources).toHaveBeenCalledWith({
                variables: {
                    input: {
                        types: [EntityType.Dataset],
                        query: '*',
                        start: 0,
                        count: 20,
                    },
                },
            }),
        );
        expect(mocks.selectProps?.filterResultsByQuery).toBe(false);

        mocks.selectProps?.onSearchChange?.('customer asset');

        expect(mocks.searchResources).toHaveBeenLastCalledWith({
            variables: {
                input: expect.objectContaining({
                    query: 'customer asset',
                    start: 0,
                    count: 20,
                }),
            },
        });
    });
});
