import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useScheduleStepSubtitle } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/syncScheduleSection/useScheduleStepSubtitle';

vi.mock('@app/ingestV2/source/builder/useIngestionSources');
vi.mock('@app/ingestV2/source/utils');
vi.mock('@app/sharedV2/forms/multiStepForm/MultiStepFormContext');

const { useIngestionSources } = await import('@app/ingestV2/source/builder/useIngestionSources');
const { getSourceConfigs, CUSTOM_SOURCE_DISPLAY_NAME } = await import('@app/ingestV2/source/utils');
const { useMultiStepContext } = await import('@app/sharedV2/forms/multiStepForm/MultiStepFormContext');

describe('useScheduleStepSubtitle', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns correct subtitle for non-custom source', async () => {
        const mockState = {
            type: 'mysql' as const,
        } as any;

        vi.mocked(useMultiStepContext).mockReturnValue({
            state: mockState,
        } as any);

        vi.mocked(useIngestionSources).mockReturnValue({
            ingestionSources: [
                {
                    type: 'mysql',
                    configs: [
                        {
                            urn: 'urn:li:dataset:mysql.test.table',
                            name: 'MySQL',
                            displayName: 'MySQL Database',
                            docsUrl: 'https://docs.mysql.com',
                            recipe: { platform: 'mysql' },
                        },
                    ],
                },
            ],
        } as any);

        vi.mocked(getSourceConfigs).mockReturnValue({
            urn: 'urn:li:dataset:mysql',
            name: 'MySQL',
            displayName: 'MySQL Database',
            docsUrl: 'https://docs.mysql.com',
            recipe: 'recipe',
        });

        const { result } = renderHook(() => useScheduleStepSubtitle());

        expect(result.current).toBe('Configure how often DataHub syncs metadata from MySQL Database.');
    });

    it('returns "this source" for custom source displayName', async () => {
        const mockState = {
            type: 'custom' as const,
        } as any;

        vi.mocked(useMultiStepContext).mockReturnValue({
            state: mockState,
        } as any);

        vi.mocked(useIngestionSources).mockReturnValue({
            ingestionSources: [],
        } as any);

        vi.mocked(getSourceConfigs).mockReturnValue({
            urn: 'urn:li:source:custom:123',
            name: 'Custom',
            displayName: CUSTOM_SOURCE_DISPLAY_NAME,
            docsUrl: '',
            recipe: 'custom',
        });

        const { result } = renderHook(() => useScheduleStepSubtitle());

        expect(result.current).toBe('Configure how often DataHub syncs metadata from this source.');
    });

    it('handles undefined sourceConfigs gracefully', async () => {
        const mockState = {
            type: 'unknown' as const,
        } as any;

        vi.mocked(useMultiStepContext).mockReturnValue({
            state: mockState,
        } as any);

        vi.mocked(useIngestionSources).mockReturnValue({
            ingestionSources: [],
        } as any);

        vi.mocked(getSourceConfigs).mockReturnValue(undefined);

        const { result } = renderHook(() => useScheduleStepSubtitle());

        expect(result.current).toBe('Configure how often DataHub syncs metadata from undefined.');
    });

    it('handles null sourceDisplayName gracefully', async () => {
        const mockState = {
            type: 'mysql' as const,
        } as any;

        vi.mocked(useMultiStepContext).mockReturnValue({
            state: mockState,
        } as any);

        vi.mocked(getSourceConfigs).mockReturnValue({
            urn: 'urn:li:dataset:mysql.test.table',
            name: 'MySQL',
            displayName: null as any,
            docsUrl: 'https://docs.mysql.com',
            recipe: 'mysql',
        });

        const { result } = renderHook(() => useScheduleStepSubtitle());

        expect(result.current).toBe('Configure how often DataHub syncs metadata from null.');
    });
});
