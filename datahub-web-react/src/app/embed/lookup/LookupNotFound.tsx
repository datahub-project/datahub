import React, { useEffect } from 'react';

import useEmbedLookupAnalytics from '@app/embed/lookup/useEmbedAnalytics';
import NonExistentEntityPage from '@app/entity/shared/entity/NonExistentEntityPage';

const LookupNotFound = ({ url }: { url: string }) => {
    const { trackLookupNotFoundEvent } = useEmbedLookupAnalytics();
    useEffect(() => {
        trackLookupNotFoundEvent(url);
    }, [trackLookupNotFoundEvent, url]);
    return <NonExistentEntityPage />;
};

export default LookupNotFound;
