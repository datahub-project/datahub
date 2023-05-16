import React from 'react';
import { useParams } from 'react-router';
import styled from 'styled-components';
import { ErrorSection } from '../../shared/error/ErrorSection';
import useGetEntityByUrl from './useGetEntityByUrl';
import LookupNotFound from './LookupNotFound';
import LookupFoundMultiple from './LookupFoundMultiple';
import LookupLoading from './LookupLoading';
import GoToLookup from './GoToLookup';

type RouteParams = {
    url: string;
};

const PageContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 85vh;
`;

// tableau test: chrome extensions -> load unpacked (unzipped dir)
// go to our tableau instance and verify it's working
const EmbedLookup = () => {
    const { url: encodedUrl } = useParams<RouteParams>();
    const decodedUrl = decodeURIComponent(encodedUrl);
    const { count, entity, error, loading } = useGetEntityByUrl(decodedUrl);

    const getContent = () => {
        if (loading) return <LookupLoading />;
        if (error) return <ErrorSection />;
        if (count === 0 || !entity) return <LookupNotFound url={encodedUrl} />;
        if (count > 1) return <LookupFoundMultiple url={encodedUrl} />;
        return <GoToLookup entityType={entity.type} entityUrn={entity.urn} />;
    };

    return <PageContainer>{getContent()}</PageContainer>;
};

export default EmbedLookup;
