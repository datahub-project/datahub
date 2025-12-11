/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ShowMoreButton } from '@app/entityV2/shared/SidebarStyledComponents';

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
