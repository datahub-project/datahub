import React from 'react';

import type { UserContextType } from '@app/context/userContext';
import { type ZendeskConfig, useZendeskWidget } from '@app/shared/hooks/useZendeskWidget';
import { useEntityRegistry } from '@src/app/useEntityRegistry';

import { AppConfig, EntityType } from '@types';

/* eslint-disable react/prop-types */

type ZendeskWidgetProps = ZendeskConfig & {
    me: UserContextType;
    config: AppConfig;
};

const getArea = () => {
    const currentLocation = window.location.href.toLowerCase();
    if (currentLocation.includes('/observe/') || currentLocation.includes('/quality/')) {
        return 'observability';
    }
    if (currentLocation.includes('/ingestion/')) {
        return 'ingestion';
    }
    if (currentLocation.includes('/settings/tokens/')) {
        return 'api';
    }
    return 'product';
};

export const ZendeskWidget: React.FC<ZendeskWidgetProps> = ({ onLoad, me, config, trigger }) => {
    const entityRegistry = useEntityRegistry();
    const userEmail = me.user?.editableProperties?.email || me.user?.info?.email || '';
    const userName =
        me.user?.editableProperties?.displayName ||
        me.user?.properties?.displayName ||
        me.user?.info?.displayName ||
        me.user?.info?.fullName ||
        entityRegistry.getDisplayName(EntityType.CorpUser, me.user?.urn);

    const area = getArea();
    const customFields = {
        // URL
        40250558726555: window.location.href,
        // Server version
        31805612950427: config?.appVersion || 'Unknown',
        // Area
        34905797479067: area,
    };
    useZendeskWidget({ onLoad, userEmail, userName, customFields, trigger });

    return null;
};
