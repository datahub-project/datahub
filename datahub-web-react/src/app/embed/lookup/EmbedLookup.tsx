import React, { useEffect } from 'react';
import { useHistory, useParams } from 'react-router';
import styled from 'styled-components';
import { LoadingOutlined } from '@ant-design/icons';
import { ErrorSection } from '../../shared/error/ErrorSection';
import useGetEntityByUrl from './useGetEntityByUrl';
import LookupNotFound from './LookupNotFound';
import LookupFoundMultiple from './LookupFoundMultiple';

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
    const { url: encodedUrl } = useParams<RouteParams>();
    const decodedUrl = decodeURIComponent(encodedUrl);
    const { embedUrl, notFound, foundMultiple, error } = useGetEntityByUrl(decodedUrl);

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
