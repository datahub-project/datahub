import React from 'react';
import { ShowMoreButton } from '../SidebarStyledComponents';

type Props = {
    totalCount: number;
    entityCount: number;
    setEntityCount: (entityCount: number) => void;
    showMaxEntity?: number;
};

export const ShowMoreSection = ({ totalCount, entityCount, setEntityCount, showMaxEntity = 4 }: Props) => {
    const showMoreCount = entityCount + showMaxEntity > totalCount ? totalCount - entityCount : showMaxEntity;
    return (
        <ShowMoreButton onClick={() => setEntityCount(entityCount + showMaxEntity)}>
            {(showMoreCount && <>show {showMoreCount} more</>) || <>show more</>}
        </ShowMoreButton>
    );
};
