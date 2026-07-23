import { renderHook } from '@testing-library/react-hooks';

import useIngestionSourcesFromData from '@app/ingestV2/shared/components/filters/hooks/useIngestionSourcesFromData';

import { ListIngestionSourcesQuery } from '@graphql/ingestion.generated';
// import { describe, it, expect } from 'vitest';
import { IngestionSource } from '@types';

// Mock data structure
// interface IngestionConfig {
//   executorId: string;
//   recipe: string;
// }

// interface IngestionSource {
//   urn: string;
//   name: string;
//   type: string;
//   config: IngestionConfig;
//   executions?: string[];
//   ownership?: string;
//   platform?: string;
//   schedule?: string;
// }

// interface ListIngestionSourcesQuery {
//   listIngestionSources?: {
//     ingestionSources?: IngestionSource[];
//   };
// }

describe('useIngestionSourcesFromData Hook', () => {
    it('returns empty array when loading is true', () => {
        const { result } = renderHook(() => useIngestionSourcesFromData(undefined, true));
        expect(result.current).toEqual([]);
    });

    it('processes ingestion sources when loading is false', () => {
        const mockSource: IngestionSource = {
            urn: 'urn:source1',
            name: 'Source 1',
            type: 'mysql',
            config: { executorId: 'default', recipe: 'recipe' },
        };
        const data: ListIngestionSourcesQuery = {
            listIngestionSources: {
                start: 0,
                count: 1,
                total: 1,
                ingestionSources: [mockSource],
            },
        };
        const { result } = renderHook(() => useIngestionSourcesFromData(data, false));

        expect(result.current).toEqual([
            {
                ...mockSource,
                executions: undefined,
                ownership: undefined,
            },
        ]);
    });

    it('returns empty array when data is undefined', () => {
        const { result } = renderHook(() => useIngestionSourcesFromData(undefined, false));
        expect(result.current).toEqual([]);
    });

    it('handles missing ingestionSources array', () => {
        const data: ListIngestionSourcesQuery = {
            listIngestionSources: {
                start: 0,
                count: 0,
                total: 0,
                ingestionSources: [],
            },
        };
        const { result } = renderHook(() => useIngestionSourcesFromData(data, false));
        expect(result.current).toEqual([]);
    });

    it('processes multiple ingestion sources correctly', () => {
        const source1: IngestionSource = {
            urn: 'urn:source1',
            name: 'Source 1',
            type: 'mysql',
            config: { executorId: 'default', recipe: 'recipe' },
        };
        const source2: IngestionSource = {
            urn: 'urn:source2',
            name: 'Source 2',
            type: 'bigquery',
            config: { executorId: 'default', recipe: 'recipe2' },
        };
        const data: ListIngestionSourcesQuery = {
            listIngestionSources: {
                start: 0,
                count: 2,
                total: 2,
                ingestionSources: [source1, source2],
            },
        };
        const { result } = renderHook(() => useIngestionSourcesFromData(data, false));

        expect(result.current).toEqual([
            { ...source1, executions: undefined, ownership: undefined },
            { ...source2, executions: undefined, ownership: undefined },
        ]);
    });
});
