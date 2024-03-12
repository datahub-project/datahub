import React, { useState } from 'react';
import { Col } from 'antd';
import { OwnershipContainer } from '../SidebarStyledComponents';
import { SidebarSection } from '../containers/profile/sidebar/SidebarSection';
import { EntityLink } from '../../../homeV2/reference/sections/EntityLink';
import { SearchResult } from '../../../../types.generated';
import { ShowMoreSection } from './ShowMoreSection';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const entityLinkTextStyle = {
    overflow: 'hidden',
    'white-space': 'nowrap',
    'text-overflow': 'ellipsis',
};

type Props = {
    ownedEntities: SearchResult[] | undefined;
};

export const UserOwnershipSidebarSection = ({ ownedEntities }: Props) => {
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const ownedEntitiesCount = ownedEntities?.length || 0;

    return (
        <SidebarSection
            title="Owner Of"
            count={ownedEntities?.length}
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
                    {ownedEntitiesCount > entityCount && (
                        <ShowMoreSection
                            totalCount={ownedEntitiesCount}
                            entityCount={entityCount}
                            setEntityCount={setEntityCount}
                            showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                        />
                    )}
                </>
            }
        />
    );
};
