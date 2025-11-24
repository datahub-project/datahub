import React from 'react';
import { Route, Switch } from 'react-router-dom';

import { ManageIngestionPage } from '@app/ingestV2/ManageIngestionPage';
import { useShowIngestionOnboardingRedesign } from '@app/ingestV2/hooks/useShowIngestionOnboardingRedesign';
import { IngestionSourceCreatePage } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceCreatePage';
import { IngestionSourceUpdatePage } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceUpdatePage';
import { PageRoutes } from '@conf/Global';

export default function IngestionRoutes() {
    const shouldShowIngestionOnboardingRedesign = useShowIngestionOnboardingRedesign();

    return (
        <Switch>
            {shouldShowIngestionOnboardingRedesign && (
                <Route path={PageRoutes.INGESTION_CREATE} render={() => <IngestionSourceCreatePage />} />
            )}
            {shouldShowIngestionOnboardingRedesign && (
                <Route path={PageRoutes.INGESTION_UPDATE} render={() => <IngestionSourceUpdatePage />} />
            )}
            <Route path={PageRoutes.INGESTION} render={() => <ManageIngestionPage />} />
        </Switch>
    );
}
