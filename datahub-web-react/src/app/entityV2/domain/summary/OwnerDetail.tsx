import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { EntityType, Owner } from '../../../../types.generated';
import CustomAvatar from '../../../shared/avatar/CustomAvatar';
import { REDESIGN_COLORS } from '../../shared/constants';

const Details = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 14px;
    font-weight: 500;
`;

const OwnerName = styled.div`
    width: 110px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

interface Props {
    owner: Owner;
}

const OwnerDetail = ({ owner }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const ownerName = entityRegistry.getDisplayName(EntityType.CorpUser, owner.owner);

    const ownerPictureLink = owner.owner.editableProperties?.pictureLink || undefined;

    const avatar: React.ReactNode = (
        <CustomAvatar name={ownerName} photoUrl={ownerPictureLink} useDefaultAvatar={false} hideTooltip />
    );

    return (
        <>
            {!!ownerName && (
                <>
                    <Details>
                        <div>{avatar}</div>
                        <Tooltip title={ownerName}>
                            <OwnerName>{ownerName}</OwnerName>
                        </Tooltip>
                    </Details>
                </>
            )}
        </>
    );
};

export default OwnerDetail;
