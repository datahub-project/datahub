import React from 'react';
import { useHistory } from 'react-router';
import { BackButton } from '../../../../../sharedV2/buttons/BackButton';

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
