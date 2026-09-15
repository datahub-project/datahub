import React from 'react';
import { useTranslation } from 'react-i18next';

import { ShowMoreButton } from '@app/entityV2/shared/SidebarStyledComponents';

type Props = {
    totalCount: number;
    entityCount: number;
    setEntityCount: (entityCount: number) => void;
    showMaxEntity?: number;
};

export const ShowMoreSection = ({ totalCount, entityCount, setEntityCount, showMaxEntity = 4 }: Props) => {
    const { t: tc } = useTranslation('common.actions');
    const showMoreCount = entityCount + showMaxEntity > totalCount ? totalCount - entityCount : showMaxEntity;
    return (
        <ShowMoreButton onClick={() => setEntityCount(entityCount + showMaxEntity)}>
            {(showMoreCount && tc('showCountMore', { count: showMoreCount })) || tc('showMore')}
        </ShowMoreButton>
    );
};
