import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { extractChartValuesFromFieldProfiles } from '@src/app/entityV2/shared/utils';
import { DatasetProfile } from '@src/types.generated';

export const useExtractMetricStats = (
    profiles: DatasetProfile[] | undefined,
    fieldPath: string | undefined,
    statName: string,
): Datum[] => {
    if (fieldPath === undefined) return [];

    const stats = extractChartValuesFromFieldProfiles(profiles ?? [], fieldPath, statName);

    return stats
        .map((stat) => ({
            x: Number(stat?.timeMs),
            y: Number(stat?.value),
        }))
        .sort((a, b) => a.x - b.x);
};
