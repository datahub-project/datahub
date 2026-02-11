import { useMemo } from 'react';

import sourcesJson from '@app/ingestV2/source/builder/sources.json';
import { SourceConfig } from '@app/ingestV2/source/builder/types';

export function useIngestionSources() {
    // TODO: replace with call to server once we have access to dynamic list of sources
    const ingestionSources: SourceConfig[] = useMemo(() => JSON.parse(JSON.stringify(sourcesJson)), []);

    return { ingestionSources };
}
