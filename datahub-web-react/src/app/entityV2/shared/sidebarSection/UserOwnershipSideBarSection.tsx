import { Col } from 'antd';
import React, { useState } from 'react';

import { OwnershipContainer, ShowMoreText } from '@app/entityV2/shared/SidebarStyledComponents';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';

import { SearchResults } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const entityLinkTextStyle = {
    overflow: 'hidden',
    'white-space': 'nowrap',
    'text-overflow': 'ellipsis',
};

type Props = {
    ownershipResults: SearchResults | undefined;
};

export const UserOwnershipSidebarSection = ({ ownershipResults }: Props) => {
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const ownedEntitiesTotal = ownershipResults?.total || 0;
    const ownedEntities = ownershipResults?.searchResults;
    const entitiesAvailableCount = ownedEntities?.length || 0;

    const hasHiddenEntities = ownedEntitiesTotal > entitiesAvailableCount;
    const isShowingMaxEntities = entityCount >= entitiesAvailableCount;
    const showAndMoreText = hasHiddenEntities && isShowingMaxEntities;

    return (
        <SidebarSection
            title="Owner Of"
            count={ownedEntitiesTotal}
            showFullCount
            content={
                <>
                    <OwnershipContainer>
                        {ownedEntities?.map((ownership, index) => {
                            const { entity } = ownership;
                            return (
                                index < entityCount && (
                                    <Col xs={11}>
                                        <EntityLink
                                            key={entity.urn}
                                            entity={entity}
                                            displayTextStyle={entityLinkTextStyle}
                                        />
                                    </Col>
                                )
                            );
                        })}
                    </OwnershipContainer>
                    {entitiesAvailableCount > entityCount && (
                        <ShowMoreSection
                            totalCount={entitiesAvailableCount}
                            entityCount={entityCount}
                            setEntityCount={setEntityCount}
                            showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                        />
                    )}
                    {showAndMoreText && (
                        <ShowMoreText>+ {ownedEntitiesTotal - entitiesAvailableCount} more</ShowMoreText>
                    )}
                </>
            }
        />
    );
};
