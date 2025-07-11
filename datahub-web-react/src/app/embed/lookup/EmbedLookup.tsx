import { LoadingOutlined } from '@ant-design/icons';
import React, { useEffect } from 'react';
import { useHistory, useParams } from 'react-router';
import styled from 'styled-components';

import LookupFoundMultiple from '@app/embed/lookup/LookupFoundMultiple';
import LookupNotFound from '@app/embed/lookup/LookupNotFound';
import useGetEntityByUrl from '@app/embed/lookup/useGetEntityByUrl';
import { urlEncodeUrn } from '@app/entity/shared/utils';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

const PageContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 85vh;
`;

const LookupLoading = styled(LoadingOutlined)`
    font-size: 50px;
`;

type RouteParams = {
    url: string;
};

const EmbedLookup = () => {
    const history = useHistory();
    const registry = useEntityRegistry();
    const { url: encodedUrl } = useParams<RouteParams>();
    const decodedUrl = decodeURIComponent(encodedUrl);
    const { entity, notFound, foundMultiple, error } = useGetEntityByUrl(decodedUrl);
    const embedUrl = entity
        ? `${PageRoutes.EMBED}/${registry.getPathName(entity.type)}/${urlEncodeUrn(entity.urn)}`
        : null;

    useEffect(() => {
        if (embedUrl) history.push(embedUrl);
    }, [embedUrl, history]);

    const getContent = () => {
        if (error) return <ErrorSection />;
        if (notFound) return <LookupNotFound url={encodedUrl} />;
        if (foundMultiple) return <LookupFoundMultiple url={encodedUrl} />;
        return <LookupLoading />;
    };

    return <PageContainer>{getContent()}</PageContainer>;
};

export default EmbedLookup;
