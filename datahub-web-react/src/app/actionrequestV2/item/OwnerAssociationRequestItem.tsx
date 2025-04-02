import { colors } from '@src/alchemy-components';
import { CustomAvatar } from '@src/app/shared/avatar';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestResult, EntityType } from '@src/types.generated';
import { UsersThree } from 'phosphor-react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import AddContentView from './AddContentView';

const OwnershipTypeContainer = styled.span`
    white-space: nowrap;
    background-color: ${colors.gray[1500]};
    border-radius: 4px;
    padding: 2px 4px;
    margin: 2px;
`;

const EntityName = styled.div`
    font-size: 12px;
    max-width: 180px;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    color: ${colors.gray[1700]};
`;

export const CreatedByContainer = styled.div<{ $isApproved?: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 2px 6px 2px 4px;
    border-radius: 20px;
    border: 1px ${(props) => (props.$isApproved ? 'solid' : 'dashed')} ${colors.gray[200]};

    :hover {
        cursor: pointer;
    }
`;

interface Props {
    actionRequest: ActionRequest;
}

const OwnerAssociationRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const owners = actionRequest.params?.ownerProposal?.owners;

    if (!owners || !owners.length) {
        return null;
    }

    const ownerViews = owners.map((ownerObj) => {
        const { owner, ownershipType } = ownerObj;
        const avatarUrl = (owner && owner?.editableProperties?.pictureLink) || '';
        return {
            primary: (
                <Link to={`${entityRegistry.getEntityUrl(owner.type, owner?.urn)}`}>
                    {owner.type === EntityType.CorpUser ? (
                        <CreatedByContainer $isApproved={actionRequest.result === ActionRequestResult.Accepted}>
                            <CustomAvatar
                                size={16}
                                photoUrl={avatarUrl}
                                name={entityRegistry.getDisplayName(EntityType.CorpUser, owner)}
                                hideTooltip
                            />
                            <EntityName>{entityRegistry.getDisplayName(owner.type, owner)}</EntityName>
                        </CreatedByContainer>
                    ) : (
                        <CreatedByContainer $isApproved={actionRequest.result === ActionRequestResult.Accepted}>
                            <UsersThree size="14" color={colors.gray[1700]} />
                            <EntityName>{entityRegistry.getDisplayName(EntityType.CorpGroup, owner)}</EntityName>
                        </CreatedByContainer>
                    )}
                </Link>
            ),
            additional: <OwnershipTypeContainer>{ownershipType?.info?.name}</OwnershipTypeContainer>,
        };
    });

    return <AddContentView requestMetadataViews={ownerViews} actionRequest={actionRequest} />;
};

export default OwnerAssociationRequestItem;
