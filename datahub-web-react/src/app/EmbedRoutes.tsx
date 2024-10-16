import React from 'react';
import { Route } from 'react-router-dom';
import { PageRoutes } from '../conf/Global';
import EmbeddedPage from './embed/EmbeddedPage';
import { useEntityRegistry } from './useEntityRegistry';
import EmbedLookup from './embed/lookup';

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
