import React from 'react';
import { useTranslation } from 'react-i18next';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlPrimaryKeyQuery } from '@graphql/mlPrimaryKey.generated';
import { EntityType } from '@types';

export const FeatureTableTab = () => {
    const entity = useBaseEntity() as GetMlPrimaryKeyQuery;
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entity.profile.tabs');

    const feature = entity && entity.mlPrimaryKey;
    const featureTables = feature?.featureTables?.relationships?.map((relationship) => relationship.entity);

    const title = t('entity.partOf', { entityName: entityRegistry.getEntityName(EntityType.MlfeatureTable) });
    return <EntityList title={title} type={EntityType.MlfeatureTable} entities={featureTables || []} />;
};
