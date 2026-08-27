import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import useFetchColumnCounts, { buildRelatedColumnFilters } from '@app/lineageV3/LineageEntityNode/Column.hooks';
import { generateIgnoreAsHops } from '@app/lineageV3/common';
import { ColumnAsset, LineageAssetType } from '@app/lineageV3/types';

import { GetColumnLineageCountsDocument } from '@graphql/lineage.generated';
import { EntityType, FilterOperator } from '@types';

const SIBLING = 'urn:li:dataset:(urn:li:dataPlatform:dbt,db.schema.customers,PROD)';

function filters(mergedUrns: Set<string>) {
    return buildRelatedColumnFilters(mergedUrns)[0].and ?? [];
}

describe('buildRelatedColumnFilters', () => {
    it('counts only direct relations, excluding columns on dbt nodes', () => {
        const and = filters(new Set());

        expect(and).toHaveLength(2);
        expect(and[0]).toEqual({ field: 'degree', values: ['1'] });
        expect(and[1]).toEqual({
            field: 'parent',
            values: ['urn:li:dataPlatform:dbt'],
            condition: FilterOperator.Contain,
            negated: true,
        });
    });

    it('excludes columns on nodes drawn as part of the same node', () => {
        const and = filters(new Set([SIBLING]));

        expect(and).toHaveLength(3);
        expect(and[2]).toEqual({ field: 'parent', values: [SIBLING], negated: true });
    });
});

const PARENT_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)';
const SCHEMA_FIELD_URN = `urn:li:schemaField:(${PARENT_URN},id)`;

/** One request's worth of counts, reusable so a second hook can only be served from the cache. */
function countsMock() {
    return {
        request: {
            query: GetColumnLineageCountsDocument,
            variables: {
                urn: SCHEMA_FIELD_URN,
                startTimeMillis: undefined,
                endTimeMillis: undefined,
                ignoreAsHops: generateIgnoreAsHops(EntityType.Dataset),
                includeSoftDeleted: false,
                orFilters: buildRelatedColumnFilters(new Set()),
            },
        },
        result: {
            data: {
                upstream: { total: 3, __typename: 'SearchAcrossLineageCounts' },
                downstream: { total: 0, __typename: 'SearchAcrossLineageCounts' },
            },
        },
    };
}

function newAsset(): ColumnAsset {
    return { name: 'id', type: LineageAssetType.Column };
}

function renderFetchCounts(asset: ColumnAsset, onDisabled = () => {}) {
    return renderHook(
        ({ lineageAsset }: { lineageAsset: ColumnAsset }) =>
            useFetchColumnCounts(PARENT_URN, SCHEMA_FIELD_URN, lineageAsset, onDisabled),
        {
            initialProps: { lineageAsset: asset },
            wrapper: ({ children }) => (
                <MemoryRouter>
                    <MockedProvider mocks={[countsMock()]}>{children as React.ReactElement}</MockedProvider>
                </MemoryRouter>
            ),
        },
    );
}

describe('useFetchColumnCounts', () => {
    it('writes the fetched counts onto the column asset', async () => {
        const asset = newAsset();
        const { result } = renderFetchCounts(asset);

        result.current.initiateRequest();

        await waitFor(() => expect(asset.numUpstream).toEqual(3));
        expect(asset.numDownstream).toEqual(0);
        expect(asset.lineageCountsFetched).toEqual(true);
    });

    it('writes the counts onto a replacement asset, as the graph rebuilds its columns', async () => {
        const asset = newAsset();
        const { result, rerender } = renderFetchCounts(asset);

        result.current.initiateRequest();
        await waitFor(() => expect(asset.numUpstream).toEqual(3));

        // A rebuilt asset carries no counts, so the column requests them again (`Column.tsx` does
        // this whenever the rendered asset has none). Only one response is mocked: the re-request
        // must be served from the cache, and `onCompleted` must write to the asset rendered now,
        // not the one that was rendered when the original request went out.
        const replacement = newAsset();
        rerender({ lineageAsset: replacement });
        result.current.initiateRequest();

        await waitFor(() => expect(replacement.numUpstream).toEqual(3));
        expect(replacement.numDownstream).toEqual(0);
        expect(asset.numUpstream).toEqual(3); // The original asset keeps its counts
    });
});
