import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { CSSObject } from 'styled-components';

import { OwnershipContainer, ShowMoreText } from '@app/entityV2/shared/SidebarStyledComponents';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';

import { SearchResults } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const entityLinkTextStyle: CSSObject = {
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    maxWidth: 150,
};

// Matches the antd Col span={11} it replaces.
const EntityColumn = styled.div`
    position: relative;
    min-height: 1px;
    flex: 0 0 45.83333333%;
    max-width: 45.83333333%;
`;

type Props = {
    ownershipResults: SearchResults | undefined;
};

export const UserOwnershipSidebarSection = ({ ownershipResults }: Props) => {
    const { t } = useTranslation('entity.shared.profile');
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const ownedEntitiesTotal = ownershipResults?.total || 0;
    const ownedEntities = ownershipResults?.searchResults;
    const entitiesAvailableCount = ownedEntities?.length || 0;

    const hasHiddenEntities = ownedEntitiesTotal > entitiesAvailableCount;
    const isShowingMaxEntities = entityCount >= entitiesAvailableCount;
    const showAndMoreText = hasHiddenEntities && isShowingMaxEntities;

    return (
        <SidebarSection
            title={t('sidebar.ownerOfTitle')}
            count={ownedEntitiesTotal}
            showFullCount
            content={
                <>
                    <OwnershipContainer>
                        {ownedEntities?.map((ownership, index) => {
                            const { entity } = ownership;
                            return (
                                index < entityCount && (
                                    <EntityColumn>
                                        <EntityLink
                                            key={entity.urn}
                                            entity={entity}
                                            displayTextStyle={entityLinkTextStyle}
                                        />
                                    </EntityColumn>
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
                        <ShowMoreText>
                            {t('sidebar.moreCount', { count: ownedEntitiesTotal - entitiesAvailableCount })}
                        </ShowMoreText>
                    )}
                </>
            }
        />
    );
};
