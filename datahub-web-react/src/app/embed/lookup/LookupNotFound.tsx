import React, { useEffect } from 'react';
import useEmbedLookupAnalytics from './useEmbedAnalytics';
import NonExistentEntityPage from '../../entity/shared/entity/NonExistentEntityPage';

const LookupNotFound = ({ url }: { url: string }) => {
    const { trackLookupNotFoundEvent } = useEmbedLookupAnalytics();
    useEffect(() => {
        trackLookupNotFoundEvent(url);
    }, [trackLookupNotFoundEvent, url]);
    return <NonExistentEntityPage />;
};

export default LookupNotFound;
