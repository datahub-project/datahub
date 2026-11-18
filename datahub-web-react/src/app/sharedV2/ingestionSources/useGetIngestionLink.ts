import { PageRoutes } from '@conf/Global';

export const useGetIngestionLink = (hasIngestionSources: boolean) => {
    return hasIngestionSources ? PageRoutes.INGESTION : PageRoutes.INGESTION_CREATE;
};
