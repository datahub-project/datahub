/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { useHistory } from 'react-router';

import { BackButton } from '@app/sharedV2/buttons/BackButton';

export const EntityBackButton = () => {
    const history = useHistory();

    // Temporary hack.
    const hasHistory = (history as any)?.length > 2;

    const onGoBack = () => {
        // TODO: Create a proper navigation history provider.
        // This can result in strange behavior when accessing via direct link.
        // Should do this before we merge.
        // PRD-766
        (history as any).goBack();
    };

    if (!hasHistory) {
        // No button to show.
        return null;
    }

    return <BackButton onGoBack={onGoBack} />;
};
