import React from 'react';
import { Route, Switch } from 'react-router-dom';

import { IngestionContextProvider } from '@app/ingestV2/IngestionContext';
import { RouteSuspense } from '@app/sharedV2/NavigationTransitions';
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
    return (
        <IngestionContextProvider>
            <RouteSuspense>
                <Switch>
                    <Route path={PageRoutes.INGESTION_CREATE} render={() => <IngestionSourceCreatePage />} />
                    <Route path={PageRoutes.INGESTION_UPDATE} render={() => <IngestionSourceUpdatePage />} />
                    <Route path={PageRoutes.INGESTION_RUN_DETAILS} render={() => <IngestionRunDetailsPage />} />
                    <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
                </Switch>
            </RouteSuspense>
        </IngestionContextProvider>
    );
}
