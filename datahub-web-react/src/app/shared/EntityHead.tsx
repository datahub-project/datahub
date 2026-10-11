import React from 'react';
import { Helmet } from 'react-helmet-async';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

export const EntityHead = () => {
    const { t } = useTranslation('shared.misc');
    const entityRegistry = useEntityRegistry();
    const { entityType, entityData } = useEntityData();

    if (!entityData) {
        return null;
    }

    const entityDisplayName = entityRegistry.getDisplayName(entityType, entityData);
    const type =
        capitalizeFirstLetterOnly(entityData?.subTypes?.typeNames?.[0]) || entityRegistry.getEntityName(entityType);

    return (
        <Helmet>
            <title>{t('entityHead.pageTitle', { entityDisplayName, type })}</title>
        </Helmet>
    );
};
