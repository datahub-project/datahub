import { renderHook } from '@testing-library/react-hooks';
import { MockedFunction, beforeEach, describe, expect, it, vi } from 'vitest';

import { useEntityHasSummaryTab } from '@app/entityV2/summary/useEntityHasSummaryTab';
import { useAppConfig } from '@app/useAppConfig';

import { EntityType } from '@types';

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

const mockedUseAppConfig = useAppConfig as MockedFunction<typeof useAppConfig>;

function withFlags(featureFlags: { assetSummaryPageV1: boolean; datasetSummaryPageV1: boolean }) {
    mockedUseAppConfig.mockReturnValue({ loaded: true, config: { featureFlags } } as any);
}

describe('useEntityHasSummaryTab', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('datasets follow datasetSummaryPageV1, not assetSummaryPageV1', () => {
        // The default config: the asset flag on, the dataset flag off. Datasets have no Summary tab.
        withFlags({ assetSummaryPageV1: true, datasetSummaryPageV1: false });
        expect(renderHook(() => useEntityHasSummaryTab(EntityType.Dataset)).result.current).toBe(false);

        withFlags({ assetSummaryPageV1: false, datasetSummaryPageV1: true });
        expect(renderHook(() => useEntityHasSummaryTab(EntityType.Dataset)).result.current).toBe(true);
    });

    it('summary-tab entities other than datasets follow assetSummaryPageV1', () => {
        withFlags({ assetSummaryPageV1: true, datasetSummaryPageV1: false });
        expect(renderHook(() => useEntityHasSummaryTab(EntityType.GlossaryTerm)).result.current).toBe(true);

        withFlags({ assetSummaryPageV1: false, datasetSummaryPageV1: true });
        expect(renderHook(() => useEntityHasSummaryTab(EntityType.GlossaryTerm)).result.current).toBe(false);
    });

    it('entity types without a summary tab never have one', () => {
        withFlags({ assetSummaryPageV1: true, datasetSummaryPageV1: true });
        expect(renderHook(() => useEntityHasSummaryTab(EntityType.CorpUser)).result.current).toBe(false);
    });
});
