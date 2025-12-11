/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
