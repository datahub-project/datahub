import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { PageRoutes } from '@conf/Global';

export const useGetIngestionLink = (hasIngestionSources: boolean) => {
    const showIngestionOnboardingRedesign = useIngestionOnboardingRedesignV1();

    let ingestionLink = PageRoutes.INGESTION;

    if (showIngestionOnboardingRedesign) {
        ingestionLink = hasIngestionSources ? PageRoutes.INGESTION : PageRoutes.INGESTION_CREATE;
    }

    return ingestionLink;
};
