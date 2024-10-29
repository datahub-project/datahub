import React, { useEffect } from 'react';
import useEmbedLookupAnalytics from './useEmbedAnalytics';
import NonExistentEntityPage from '../../entity/shared/entity/NonExistentEntityPage';

const LookupFoundMultiple = ({ url }: { url: string }) => {
    const { trackLookupMultipleFoundEvent } = useEmbedLookupAnalytics();
    useEffect(() => {
        trackLookupMultipleFoundEvent(url);
    }, [trackLookupMultipleFoundEvent, url]);
    return <NonExistentEntityPage />;
};

export default LookupFoundMultiple;
