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

const EmbedLookup = () => {
    const { url: encodedUrl } = useParams<RouteParams>();
    const decodedUrl = decodeURIComponent(encodedUrl);
    const lookup = useGetEntityByUrl(decodedUrl);

    const getContent = () => {
        if (lookup.loading) return <LookupLoading />;
        if (lookup.error) return <ErrorSection />;
        if (lookup.count === 0 || !lookup.entity) return <LookupNotFound url={encodedUrl} />;
        if (lookup.count > 1) return <LookupFoundMultiple url={encodedUrl} />;
        return <GoToLookup entityType={lookup.entity.type} entityUrn={lookup.entity.urn} />;
    };

    return <PageContainer>{getContent()}</PageContainer>;
};

export default EmbedLookup;
