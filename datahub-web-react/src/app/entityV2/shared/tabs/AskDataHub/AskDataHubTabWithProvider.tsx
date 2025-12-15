import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { AskDataHubProvider } from '@app/entityV2/shared/tabs/AskDataHub/AskDataHubContext';
import AskDataHubTab from '@app/entityV2/shared/tabs/AskDataHub/AskDataHubTab';
import { EntityTabProps } from '@app/entityV2/shared/types';

/**
 * Thin wrapper that keeps the AskDataHub provider colocated with the tab.
 * Moved out of utils per review to avoid scattering providers and to scope
 * the AskDataHub context only to this tab’s usage.
 */
const AskDataHubTabWithProvider: React.FC<EntityTabProps> = (props) => {
    const entityContext = useEntityData();
    const entityUrn = entityContext?.urn || '';

    return (
        <AskDataHubProvider entityUrn={entityUrn}>
            <AskDataHubTab {...props} />
        </AskDataHubProvider>
    );
};

export default AskDataHubTabWithProvider;
