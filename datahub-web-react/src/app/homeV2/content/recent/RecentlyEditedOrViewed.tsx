import React from 'react';
import { useTranslation } from 'react-i18next';

import { EntityCardList } from '@app/homeV2/content/recent/EntityCardList';

import { Entity } from '@types';

type Props = {
    entities: Entity[];
};

export const RecentlyEditedOrViewed = ({ entities }: Props) => {
    const { t } = useTranslation('home.v2');
    return <EntityCardList title={t('recentlyViewed.title')} entities={entities} isHomePage />;
};
