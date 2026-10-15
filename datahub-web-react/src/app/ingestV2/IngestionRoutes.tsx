import React, { Suspense } from 'react';
import { Route, Switch } from 'react-router-dom';

import { IngestionContextProvider } from '@app/ingestV2/IngestionContext';
import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { SuspenseBlock } from '@app/sharedV2/SuspenseBlock';
import { lazyDefault, lazyNamed } from '@app/sharedV2/lazyPage';
import { PageRoutes } from '@conf/Global';

const ManageIngestionPage = lazyNamed(() => import('@app/ingestV2/ManageIngestionPage'), 'ManageIngestionPage');
const IngestionRunDetailsPage = lazyDefault(() => import('@app/ingestV2/runDetails/IngestionRunDetailsPage'));
const IngestionSourceCreatePage = lazyNamed(
    () => import('@app/ingestV2/source/multiStepBuilder/IngestionSourceCreatePage'),
    'IngestionSourceCreatePage',
);
const IngestionSourceUpdatePage = lazyNamed(
    () => import('@app/ingestV2/source/multiStepBuilder/IngestionSourceUpdatePage'),
    'IngestionSourceUpdatePage',
);

export default function IngestionRoutes() {
    const shouldShowIngestionOnboardingRedesignV1 = useIngestionOnboardingRedesignV1();

    return (
        <IngestionContextProvider>
            <Suspense fallback={<SuspenseBlock />}>
                <Switch>
                    {shouldShowIngestionOnboardingRedesignV1 && (
                        <Route path={PageRoutes.INGESTION_CREATE} render={() => <IngestionSourceCreatePage />} />
                    )}
                    {shouldShowIngestionOnboardingRedesignV1 && (
                        <Route path={PageRoutes.INGESTION_UPDATE} render={() => <IngestionSourceUpdatePage />} />
                    )}
                    {shouldShowIngestionOnboardingRedesignV1 && (
                        <Route path={PageRoutes.INGESTION_RUN_DETAILS} render={() => <IngestionRunDetailsPage />} />
                    )}
                    <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                </Switch>
            </Suspense>
        </IngestionContextProvider>
    );
}
