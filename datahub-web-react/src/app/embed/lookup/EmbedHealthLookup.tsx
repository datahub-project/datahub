import React, { useEffect } from 'react';
import { useHistory, useParams } from 'react-router';

import useGetEntityByUrl from '@app/embed/lookup/useGetEntityByUrl';
import NonExistentEntityPage from '@app/entity/shared/entity/NonExistentEntityPage';
import { urlEncodeUrn } from '@app/entity/shared/utils';
import { PageRoutes } from '@conf/Global';

type RouteParams = {
    url: string;
};

const EmbedHealthLookup = () => {
    const history = useHistory();
    const { url: encodedUrl } = useParams<RouteParams>();
    const decodedUrl = decodeURIComponent(encodedUrl);
    const { entity, notFound, foundMultiple, error } = useGetEntityByUrl(decodedUrl);
    const embedHealthUrl = entity ? `${PageRoutes.EMBED_HEALTH}/${urlEncodeUrn(entity.urn)}` : null;

    useEffect(() => {
        if (embedHealthUrl) history.push(embedHealthUrl);
    }, [embedHealthUrl, history]);

    if (error || notFound || foundMultiple) {
        return <NonExistentEntityPage />;
    }
    return null;
};

export default EmbedHealthLookup;
