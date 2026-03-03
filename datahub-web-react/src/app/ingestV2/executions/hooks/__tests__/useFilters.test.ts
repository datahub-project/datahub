import { renderHook } from '@testing-library/react-hooks';

import { CLI_EXECUTOR_ID } from '@app/ingestV2/constants';
import { EXECUTOR_TYPE_FIELD, INGESTION_SOURCE_FIELD } from '@app/ingestV2/executions/components/Filters';
import useFilters from '@app/ingestV2/executions/hooks/useFilters';
import {
    EXECUTOR_TYPE_ALL_VALUE,
    EXECUTOR_TYPE_CLI_VALUE,
} from '@app/ingestV2/shared/components/filters/ExecutorTypeFilter';

describe('useFilters Hook', () => {
    it('returns no filters when appliedFilters is empty', () => {
        const appliedFilters = new Map();
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current).toEqual({ filters: [], hasAppliedFilters: false });
    });

    it('adds executor filter for CLI type', () => {
        const appliedFilters = new Map([[EXECUTOR_TYPE_FIELD, [EXECUTOR_TYPE_CLI_VALUE]]]);
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current.filters).toEqual([
            {
                field: 'executorId',
                values: [CLI_EXECUTOR_ID],
                negated: false,
            },
        ]);
        expect(result.current.hasAppliedFilters).toBe(true);
    });

    it('adds negated executor filter for non-CLI type', () => {
        const appliedFilters = new Map([[EXECUTOR_TYPE_FIELD, ['other']]]);
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current.filters).toEqual([
            {
                field: 'executorId',
                values: [CLI_EXECUTOR_ID],
                negated: true,
            },
        ]);
    });

    it('ignores "all" executor type', () => {
        const appliedFilters = new Map([[EXECUTOR_TYPE_FIELD, [EXECUTOR_TYPE_ALL_VALUE]]]);
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current.filters).toEqual([]);
        expect(result.current.hasAppliedFilters).toBe(false);
    });

    it('adds ingestion source filter', () => {
        const sourceUrns = ['urn1', 'urn2'];
        const appliedFilters = new Map([[INGESTION_SOURCE_FIELD, sourceUrns]]);
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current.filters).toEqual([
            {
                field: INGESTION_SOURCE_FIELD,
                values: sourceUrns,
            },
        ]);
    });

    it('combines executor and ingestion source filters', () => {
        const sourceUrns = ['urn1'];
        const appliedFilters = new Map([
            [EXECUTOR_TYPE_FIELD, [EXECUTOR_TYPE_CLI_VALUE]],
            [INGESTION_SOURCE_FIELD, sourceUrns],
        ]);
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current.filters).toEqual([
            {
                field: 'executorId',
                values: [CLI_EXECUTOR_ID],
                negated: false,
            },
            {
                field: INGESTION_SOURCE_FIELD,
                values: sourceUrns,
            },
        ]);
    });

    it('ignores empty ingestion source filter', () => {
        const appliedFilters = new Map([[INGESTION_SOURCE_FIELD, []]]);
        const { result } = renderHook(() => useFilters(appliedFilters));
        expect(result.current.filters).toEqual([]);
        expect(result.current.hasAppliedFilters).toBe(false);
    });

    it('updates filters when appliedFilters changes', () => {
        const initialFilters = new Map();
        const { rerender, result } = renderHook(({ filters }) => useFilters(filters), {
            initialProps: { filters: initialFilters },
        });
        expect(result.current.hasAppliedFilters).toBe(false);

        const updatedFilters = new Map([[INGESTION_SOURCE_FIELD, ['urn1']]]);
        rerender({ filters: updatedFilters });
        expect(result.current.hasAppliedFilters).toBe(true);
        expect(result.current.filters).toHaveLength(1);
    });
});
