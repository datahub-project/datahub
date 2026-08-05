import { Avatar, Tooltip } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { message } from 'antd';
import React, { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';
import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';

import { AssertionListTableRow } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { SelectItemPopover } from '@src/alchemy-components/components/SelectItemsPopover';
import { useBatchAddOwnersMutation, useBatchRemoveOwnersMutation } from '@src/graphql/mutations.generated';
import { CorpUser, Entity, EntityType, OwnerEntityType } from '@src/types.generated';

type OwnerOptionProps = {
    option: { value: string; label: React.ReactNode | string; item?: Entity };
};

function OwnerOption({ option }: OwnerOptionProps) {
    const entityRegistry = useEntityRegistryV2();
    const { item } = option;
    if (!item) return <>{option.label}</>;
    const avatarUrl =
        item.type === EntityType.CorpUser ? (item as CorpUser).editableProperties?.pictureLink || undefined : undefined;
    return (
        <Avatar
            name={entityRegistry.getDisplayName(item.type, item)}
            imageUrl={avatarUrl}
            showInPill
            type={mapEntityTypeToAvatarType(item.type)}
        />
    );
}

const Container = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    cursor: pointer;
`;

const AddButton = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 24px;
    width: 24px;
    color: ${(props) => props.theme.colors.icon};
    border-radius: 100px;
    transition: all 0.2s;
    &:hover {
        color: ${(props) => props.theme.colors.text};
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
`;

type Props = {
    record: AssertionListTableRow;
    refetch?: () => void;
};

export const AcrylAssertionOwnerColumn = ({ record, refetch }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const [popoverVisible, setPopoverVisible] = useState(false);
    const isSaving = useRef(false);
    const entityRegistry = useEntityRegistryV2();
    const canEditOwners = !!record.assertion.dataset?.privileges?.canEditAssertionOwners;

    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [batchRemoveOwnersMutation] = useBatchRemoveOwnersMutation();

    const { recommendedData: allActors } = useGetRecommendations([EntityType.CorpUser, EntityType.CorpGroup]);
    const { defaultOwnershipTypeUrn } = useOwnershipTypes();

    const owners = record.ownership?.owners ?? [];
    const selectedOwnerEntities = owners.map((o) => o.owner);

    const singleOwner = owners.length === 1 ? owners[0].owner : undefined;

    const ownerAvatars = (
        <>
            {singleOwner && (
                <Link
                    to={entityRegistry.getEntityUrl(singleOwner.type, singleOwner.urn)}
                    onClick={(e) => e.stopPropagation()}
                >
                    <Avatar
                        name={entityRegistry.getDisplayName(singleOwner.type, singleOwner)}
                        imageUrl={singleOwner.editableProperties?.pictureLink}
                        showInPill
                        type={mapEntityTypeToAvatarType(singleOwner.type)}
                    />
                </Link>
            )}
            {owners.length > 1 && (
                <AvatarStackWithHover
                    avatars={owners.map((owner) => ({
                        name: entityRegistry.getDisplayName(owner.owner.type, owner.owner),
                        imageUrl: owner.owner.editableProperties?.pictureLink,
                        type: mapEntityTypeToAvatarType(owner.owner.type),
                        urn: owner.owner.urn,
                    }))}
                    showRemainingNumber
                    maxToShow={2}
                    entityRegistry={entityRegistry}
                />
            )}
        </>
    );

    if (!canEditOwners) {
        return owners.length > 0 ? <Container>{ownerAvatars}</Container> : null;
    }

    const handleContainerClick = (e: React.MouseEvent) => {
        e.stopPropagation();
    };

    const handleClosePopover = () => {
        setPopoverVisible(false);
    };

    const getOwnerEntityType = (ownerUrn: string): OwnerEntityType =>
        ownerUrn.startsWith('urn:li:corpGroup:') ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;

    const handleSelectionChange = async ({
        selectedItems: addedUrns,
        removedItems: removedUrns,
    }: {
        selectedItems: string[];
        removedItems: string[];
    }) => {
        if (!addedUrns?.length && !removedUrns?.length) return;
        if (isSaving.current) return;
        isSaving.current = true;
        try {
            if (addedUrns?.length) {
                await batchAddOwnersMutation({
                    variables: {
                        input: {
                            owners: addedUrns.map((urn) => ({
                                ownerUrn: urn,
                                ownerEntityType: getOwnerEntityType(urn),
                                ownershipTypeUrn: defaultOwnershipTypeUrn,
                            })),
                            resources: [{ resourceUrn: record.urn }],
                        },
                    },
                });
            }
            if (removedUrns?.length) {
                await batchRemoveOwnersMutation({
                    variables: {
                        input: {
                            ownerUrns: removedUrns,
                            resources: [{ resourceUrn: record.urn }],
                        },
                    },
                });
            }
            message.success(t('builder.details.ownersUpdated'), 2);
            setPopoverVisible(false);
            refetch?.();
        } catch (e) {
            message.error(handleBatchError([record.urn], e, t('builder.details.failedUpdateOwners')));
        } finally {
            isSaving.current = false;
        }
    };

    const renderOption = (option: { value: string; label: React.ReactNode | string; item?: Entity }) => (
        <OwnerOption option={option} />
    );

    return (
        <SelectItemPopover
            key={`${popoverVisible}`}
            entities={allActors || []}
            selectedItems={selectedOwnerEntities}
            refetch={refetch}
            onClose={handleClosePopover}
            entityTypes={[EntityType.CorpUser, EntityType.CorpGroup]}
            handleSelectionChange={handleSelectionChange}
            visible={popoverVisible}
            onVisibleChange={setPopoverVisible}
            renderOption={renderOption}
        >
            {/* eslint-disable-next-line jsx-a11y/click-events-have-key-events, jsx-a11y/no-static-element-interactions */}
            <Container onClick={handleContainerClick}>
                {ownerAvatars}
                <Tooltip title={t('builder.details.ownersTooltip')}>
                    <AddButton>{owners.length > 0 ? <PencilSimple /> : <Plus />}</AddButton>
                </Tooltip>
            </Container>
        </SelectItemPopover>
    );
};
