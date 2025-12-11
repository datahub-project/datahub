/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import React, { useEffect } from 'react';
import { useHistory, useParams } from 'react-router';
import styled from 'styled-components';

import LookupFoundMultiple from '@app/embed/lookup/LookupFoundMultiple';
import LookupNotFound from '@app/embed/lookup/LookupNotFound';
import useGetEntityByUrl from '@app/embed/lookup/useGetEntityByUrl';
import { ErrorSection } from '@app/shared/error/ErrorSection';

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
