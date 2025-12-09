import React from 'react';
import { Route, Switch } from 'react-router-dom';

import { ManageIngestionPage } from '@app/ingestV2/ManageIngestionPage';
import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import IngestionRunDetailsPage from '@app/ingestV2/runDetails/IngestionRunDetailsPage';
import { IngestionSourceCreatePage } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceCreatePage';
import { IngestionSourceUpdatePage } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceUpdatePage';
import { PageRoutes } from '@conf/Global';

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
            {shouldShowIngestionOnboardingRedesignV1 && (
                <Route path={PageRoutes.INGESTION_RUN_DETAILS} render={() => <IngestionRunDetailsPage />} />
            )}
            <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
        </Switch>
    );
}
