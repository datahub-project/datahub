import React from 'react';
import { Route } from 'react-router-dom';

import EmbeddedPage from '@app/embed/EmbeddedPage';
import EmbedLookup from '@app/embed/lookup';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

export default function EmbedRoutes() {
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <Route exact path={PageRoutes.EMBED_LOOKUP} render={() => <EmbedLookup />} />
            {entityRegistry.getEntities().map((entity) => (
                <Route
                    key={`${entity.getPathName()}/${PageRoutes.EMBED}`}
                    path={`${PageRoutes.EMBED}/${entity.getPathName()}/:urn`}
                    render={() => <EmbeddedPage entityType={entity.type} />}
                />
            ))}
        </>
    );
}
