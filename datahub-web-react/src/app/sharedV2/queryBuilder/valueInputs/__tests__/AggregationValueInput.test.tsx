/* eslint-disable react/no-unused-prop-types -- mock Select only captures props */
import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import AggregationValueInput from '@app/sharedV2/queryBuilder/valueInputs/AggregationValueInput';

import { EntityType } from '@types';

type CapturedSelect = { options: { value: string; label: string }[] };

const mocks = vi.hoisted(() => ({
    fetchAggregations: vi.fn(),
    data: undefined as unknown,
    selectProps: undefined as CapturedSelect | undefined,
    resolvedEntities: [] as unknown[],
}));

vi.mock('@app/sharedV2/useGetEntities', () => ({
    useGetEntities: (urns: string[]) => ({
        entities: mocks.resolvedEntities.filter((e: any) => urns.includes(e.urn)),
        loading: false,
    }),
}));

vi.mock('@components', async () => ({
    ...(await vi.importActual<typeof import('@components')>('@components')),
    Select: (props: CapturedSelect) => {
        mocks.selectProps = props;
        return <div />;
    },
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: (type: EntityType, entity: any) =>
            type === EntityType.DataPlatform ? entity.name : entity.instanceId,
    }),
}));

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useAggregateAcrossEntitiesLazyQuery: () => [mocks.fetchAggregations, { data: mocks.data, loading: false }],
}));

const instance = (platform: string) => ({
    value: `urn:li:dataPlatformInstance:(urn:li:dataPlatform:${platform},prod)`,
    count: 1,
    entity: {
        urn: `urn:li:dataPlatformInstance:(urn:li:dataPlatform:${platform},prod)`,
        type: EntityType.DataPlatformInstance,
        instanceId: 'prod',
        platform: { urn: `urn:li:dataPlatform:${platform}`, type: EntityType.DataPlatform, name: platform },
    },
});

describe('AggregationValueInput', () => {
    it('requests more than the default 20 values and disambiguates instances by platform', async () => {
        mocks.data = {
            aggregateAcrossEntities: {
                facets: [{ field: 'platformInstance', aggregations: [instance('snowflake'), instance('bigquery')] }],
            },
        };

        render(
            <AggregationValueInput
                facetField="platformInstance"
                selectedValues={[]}
                onChangeSelectedValues={vi.fn()}
            />,
        );

        await waitFor(() =>
            expect(mocks.fetchAggregations).toHaveBeenCalledWith({
                variables: {
                    input: expect.objectContaining({ searchFlags: { maxAggValues: expect.any(Number) } }),
                },
            }),
        );
        expect(mocks.fetchAggregations.mock.calls[0][0].variables.input.searchFlags.maxAggValues).toBeGreaterThan(20);
        expect(mocks.selectProps?.options.map((o) => o.label)).toEqual(['snowflake prod', 'bigquery prod']);
    });

    it('shows a selected value missing from the facet by its entity name, not its URN', () => {
        const urn = 'urn:li:dataPlatform:mysql';
        mocks.data = { aggregateAcrossEntities: { facets: [{ field: 'platform', aggregations: [] }] } };
        mocks.resolvedEntities = [{ urn, type: EntityType.DataPlatform, name: 'MySQL' }];

        render(<AggregationValueInput facetField="platform" selectedValues={[urn]} onChangeSelectedValues={vi.fn()} />);

        expect(mocks.selectProps?.options).toEqual([{ value: urn, label: 'MySQL' }]);
    });
});
