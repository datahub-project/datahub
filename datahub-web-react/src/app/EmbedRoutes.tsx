import React, { Suspense } from 'react';
import { Route } from 'react-router-dom';

import { SuspenseBlock } from '@app/sharedV2/SuspenseBlock';
import { lazyDefault } from '@app/sharedV2/lazyPage';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

const EmbeddedPage = lazyDefault(() => import('@app/embed/EmbeddedPage'));
const EmbedLookup = lazyDefault(() => import('@app/embed/lookup'));

export default function EmbedRoutes() {
    const entityRegistry = useEntityRegistry();

    return (
        <Suspense fallback={<SuspenseBlock />}>
            <Route exact path={PageRoutes.EMBED_LOOKUP} render={() => <EmbedLookup />} />
            {entityRegistry.getEntities().map((entity) => (
                <Route
                    key={`${entity.getPathName()}/${PageRoutes.EMBED}`}
                    path={`${PageRoutes.EMBED}/${entity.getPathName()}/:urn`}
                    render={() => <EmbeddedPage entityType={entity.type} />}
                />
            ))}
        </Suspense>
    );
}
