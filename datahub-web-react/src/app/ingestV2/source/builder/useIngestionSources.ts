import { TFunction } from 'i18next';
import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import sourcesJson from '@app/ingestV2/source/builder/sources.json';
import { SourceConfig } from '@app/ingestV2/source/builder/types';

const CATEGORY_KEYS: Record<string, string> = {
    'BI & Analytics': 'category.biAndAnalytics',
    'BI Tool': 'category.biTool',
    'Context Document Sources': 'category.contextDocumentSources',
    'Data Collector': 'category.dataCollector',
    'Data Lake': 'category.dataLake',
    'Data Warehouse': 'category.dataWarehouse',
    Database: 'category.database',
    'ETL / ELT': 'category.etlElt',
    'ML Platforms': 'category.mlPlatforms',
    Miscellaneous: 'category.miscellaneous',
    Orchestration: 'category.orchestration',
    'Query Engine': 'category.queryEngine',
};

function toSourceKey(name: string): string {
    return name.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
}

function resolveSource(source: SourceConfig, t: TFunction<'ingest.sources'>): SourceConfig {
    const key = toSourceKey(source.name);
    return {
        ...source,
        displayName: t(`sources.${key}.displayName`, { defaultValue: source.displayName }),
        description: source.description
            ? t(`sources.${key}.description`, { defaultValue: source.description })
            : source.description,
        category: source.category
            ? t(CATEGORY_KEYS[source.category] ?? source.category, { defaultValue: source.category })
            : source.category,
    };
}

export function useIngestionSources() {
    const { t, i18n } = useTranslation('ingest.sources');
    // TODO: replace with call to server once we have access to dynamic list of sources
    const ingestionSources: SourceConfig[] = useMemo(
        () => (JSON.parse(JSON.stringify(sourcesJson)) as SourceConfig[]).map((s) => resolveSource(s, t)),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [t, i18n.language],
    );

    return { ingestionSources };
}
