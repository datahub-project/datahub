import React from 'react';
import styled from 'styled-components';
import { CreatedByContainer } from '@src/app/govern/structuredProperties/styledComponents';
import { UsersThree } from 'phosphor-react';
import { colors } from '@src/alchemy-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import CustomAvatar from '@src/app/shared/avatar/CustomAvatar';
import { Link } from 'react-router-dom';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';
import AddContentView from './AddContentView';
import { ActionRequest, EntityType } from '../../../types.generated';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

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

const REQUEST_TYPE_DISPLAY_NAME = 'Owner Proposal';

/**
 * A list item representing a Owner association request.
 */
export default function OwnerAssociationRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
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
                        <CreatedByContainer>
                            <CustomAvatar
                                size={16}
                                photoUrl={avatarUrl}
                                name={entityRegistry.getDisplayName(EntityType.CorpUser, owner)}
                                hideTooltip
                            />
                            <EntityName>{entityRegistry.getDisplayName(owner.type, owner)}</EntityName>
                        </CreatedByContainer>
                    ) : (
                        <CreatedByContainer>
                            <UsersThree size="14" color={colors.gray[1700]} />
                            <EntityName>{entityRegistry.getDisplayName(EntityType.CorpGroup, owner)}</EntityName>
                        </CreatedByContainer>
                    )}
                </Link>
            ),
            additional: <OwnershipTypeContainer>{ownershipType?.info?.name}</OwnershipTypeContainer>,
        };
    });

    const contentView = <AddContentView requestMetadataViews={ownerViews} actionRequest={actionRequest} />;

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestContentView={contentView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
            showActionsButtons={showActionsButtons}
        />
    );
}
