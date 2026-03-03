import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import analytics from '@app/analytics';
import { EventType } from '@app/analytics/event';
import { IconStyleType } from '@app/entity/Entity';
import { useIsBrowseV2 } from '@app/search/useSearchAndBrowseVersion';
import { ENTITY_SUB_TYPE_FILTER_NAME } from '@app/search/utils/constants';
import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';
import { LogoCountCard } from '@app/shared/LogoCountCard';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const BrowseEntityCardWrapper = styled.div``;

export const BrowseEntityCard = ({ entityType, count }: { entityType: EntityType; count: number }) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const showBrowseV2 = useIsBrowseV2();
    const entityPathName = entityRegistry.getPathName(entityType);
    const customCardUrlPath = entityRegistry.getCustomCardUrlPath(entityType);
    const url = customCardUrlPath || `${PageRoutes.BROWSE}/${entityPathName}`;
    const onBrowseEntityCardClick = () => {
        analytics.event({
            type: EventType.HomePageBrowseResultClickEvent,
            entityType,
        });
    };

    function browse() {
        if (showBrowseV2 && !customCardUrlPath) {
            navigateToSearchUrl({
                query: '*',
                filters: [{ field: ENTITY_SUB_TYPE_FILTER_NAME, values: [entityType] }],
                history,
            });
        } else {
            history.push(url);
        }
    }

    return (
        <BrowseEntityCardWrapper onClick={browse} data-testid={`entity-type-browse-card-${entityType}`}>
            <LogoCountCard
                logoComponent={entityRegistry.getIcon(entityType, 18, IconStyleType.HIGHLIGHT)}
                name={entityRegistry.getCollectionName(entityType)}
                count={count}
                onClick={onBrowseEntityCardClick}
            />
        </BrowseEntityCardWrapper>
    );
};
