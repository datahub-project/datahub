import React, { useEffect } from 'react';

import useEmbedLookupAnalytics from '@app/embed/lookup/useEmbedAnalytics';
import NonExistentEntityPage from '@app/entity/shared/entity/NonExistentEntityPage';

const LookupFoundMultiple = ({ url }: { url: string }) => {
    const { trackLookupMultipleFoundEvent } = useEmbedLookupAnalytics();
    useEffect(() => {
        trackLookupMultipleFoundEvent(url);
    }, [trackLookupMultipleFoundEvent, url]);
    return <NonExistentEntityPage />;
};

export default LookupFoundMultiple;
