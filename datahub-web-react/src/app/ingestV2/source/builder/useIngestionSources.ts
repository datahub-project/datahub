import { TFunction } from 'i18next';
import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import sourcesJson from '@app/ingestV2/source/builder/sources.json';
import { SourceConfig } from '@app/ingestV2/source/builder/types';

function toSourceKey(name: string): string {
    return name.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
}

function resolveSource(source: SourceConfig, t: TFunction<'ingest.sources'>): SourceConfig {
    const key = toSourceKey(source.name);
    return {
        ...source,
        // displayName intentionally NOT translated — source/connector display names are proper
        // nouns (Athena, BigQuery, Confluence, …) and must stay identical across all languages.
        description: source.description
            ? t(`sources.${key}.description`, { defaultValue: source.description })
            : source.description,
        // category intentionally kept as the original English value — it is used as a grouping
        // key in utils.ts which compares against hardcoded English category name constants.
        // Category labels are translated at the render site in SelectSourceStep.tsx.
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
