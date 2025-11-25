import React from 'react';
import { Route, Switch } from 'react-router-dom';

import { ManageIngestionPage } from '@app/ingestV2/ManageIngestionPage';
import { IngestionSourceCreatePage } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceCreatePage';
import { IngestionSourceUpdatePage } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceUpdatePage';
import { PageRoutes } from '@conf/Global';
import { useIngestionOnboardingRedesignV1 } from './hooks/useIngestionOnboardingRedesignV1';

export default function IngestionRoutes() {
    const shouldShowIngestionOnboardingRedesignV1 = useIngestionOnboardingRedesignV1();

    return (
        <Switch>
            {shouldShowIngestionOnboardingRedesignV1 && (
                <Route path={PageRoutes.INGESTION_CREATE} render={() => <IngestionSourceCreatePage />} />
            )}
            {shouldShowIngestionOnboardingRedesignV1 && (
                <Route path={PageRoutes.INGESTION_UPDATE} render={() => <IngestionSourceUpdatePage />} />
            )}
            <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
        </Switch>
    );
}
