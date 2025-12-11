/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { EntityNameList } from '@app/recommendations/renderer/component/EntityNameList';
import { RecommendationDisplayType, RecommendationRenderProps } from '@app/recommendations/types';
import { recommendationClickEvent } from '@app/recommendations/util/recommendationClickEvent';

import { Entity } from '@types';

export const EntityNameListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
    displayType,
}: RecommendationRenderProps) => {
    const entities = content.map((cnt) => cnt.entity).filter((entity) => entity !== undefined && entity !== null);
    const EntityNameListComponent =
        displayType === RecommendationDisplayType.COMPACT ? CompactEntityNameList : EntityNameList;
    return (
        <EntityNameListComponent
            onClick={(index) =>
                recommendationClickEvent({
                    renderId,
                    moduleId,
                    scenarioType,
                    renderType,
                    index,
                })
            }
            entities={entities as Array<Entity>}
        />
    );
};
