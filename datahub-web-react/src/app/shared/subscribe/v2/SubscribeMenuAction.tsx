import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useIsSeparateSiblingsMode } from '@src/app/entityV2/shared/useIsSeparateSiblingsMode';
import styled from 'styled-components/macro';
import { Dropdown, MenuProps } from 'antd';
import { Pill, Text } from '@src/alchemy-components';
import { useEntityData, useMutationUrn } from '@src/app/entity/shared/EntityContext';
import { Bell } from '@phosphor-icons/react';
import TooltipHeader, { SubTitle } from '@src/alchemy-components/components/Tooltip2/TooltipHeader';
import { Tooltip2 } from '@src/alchemy-components/components/Tooltip2';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { CreatedByContainer } from '@src/app/govern/structuredProperties/styledComponents';
import { UsersThree } from 'phosphor-react';
import colors from '@src/alchemy-components/theme/foundations/colors';
import useSubscription from '../useSubscription';
import { formatNumber } from '../../formatNumber';
import CustomAvatar from '../../avatar/CustomAvatar';
import { ShowMoreButton } from '../../ShowMoreSection';
import useSubscriptionSummary from '../useSubscriptionSummary';
import useGroupRelationships from '../useGroupRelationships';
import SubscriptionDrawer from '../drawer/SubscriptionDrawer';
import useDeleteSubscription from '../useDeleteSubscription';
import { ActionMenuItem } from '../../../entityV2/shared/EntityDropdown/styledComponents';
import { EntityType } from '../../../../types.generated';
import Loading from '../../Loading';

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

const PillsContainer = styled.div`
    width: 100%;
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    max-height: 100px;
    overflow-y: auto;

    &::-webkit-scrollbar {
        width: 4px;
    }
`;

const IconContainer = styled.div`
    flex-shrink: 0;
    display: flex;
    align-items: center;
    border: 1px red solid;
    padding: 10px;
`;

export const SubscribeMenuAction = () => {
    const entityRegistry = useEntityRegistry();
    const { entityData, entityType } = useEntityData();
    const entityUrn = useMutationUrn();

    const entityName = entityData?.name || '';
    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();
    const isEntityExists = entityType === EntityType.Dataset ? entityData?.exists : true;

    const { hasGroupRelationships } = useGroupRelationships({ count: 1 });
    const { subscription, isSubscribed, canManageSubscription, refetchSubscription } = useSubscription({
        isPersonal,
        entityUrn,
        groupUrn,
        isEntityExists,
    });

    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const isSiblingMode = (entityData?.siblingsSearch?.total && !isSeparateSiblingsMode) || false;

    const {
        isUserSubscribed,
        numUserSubscriptions,
        numGroupSubscriptions,
        subscribedGroups,
        subscribedUsers,
        setIsUserSubscribed,
        refetchSubscriptionSummary,
        fetchMoreGroups,
        fetchMoreUsers,
        isFetchingSubscriptionSummary,
    } = useSubscriptionSummary({ entityUrn, isEntityExists });

    const renderSubscribeIcon = () => {
        if (isFetchingSubscriptionSummary) {
            return (
                <IconContainer data-testid="subscribe-action">
                    <Loading height={20} marginTop={0} />
                </IconContainer>
            );
        }
        if (isUserSubscribed) {
            return (
                <IconContainer data-testid="subscribe-action">
                    <Bell weight="fill" size={14} />
                </IconContainer>
            );
        }
        return (
            <IconContainer data-testid="subscribe-action">
                <Bell />
            </IconContainer>
        );
    };

    const handleUpsertSubscription = () => setIsUserSubscribed(true);

    const refetch = () => {
        refetchSubscription();
        refetchSubscriptionSummary();
    };

    const deleteSubscription = useDeleteSubscription({
        subscription,
        isPersonal,
        onDeleteSuccess: () => setIsUserSubscribed(false),
        onRefetch: refetch,
    });

    const onClickMenuItem: MenuProps['onClick'] = ({ key }) => {
        if (key === DROPDOWN_KEYS.SUBSCRIBE_ME) {
            setIsPersonal(true);
            setIsDrawerOpen(true);
        } else if (key === DROPDOWN_KEYS.SUBSCRIBE_GROUP) {
            setIsPersonal(false);
            setGroupUrn(undefined);
            setIsDrawerOpen(true);
        } else if (key === DROPDOWN_KEYS.UNSUBSCRIBE_ME) {
            setIsPersonal(true);
            deleteSubscription();
        }
    };

    const onCloseDrawer = () => {
        setIsDrawerOpen(false);
        setIsPersonal(true);
        setGroupUrn(undefined);
    };

    return (
        <ActionMenuItem key="subscribe">
            <Dropdown
                disabled={isSiblingMode}
                trigger={['click']}
                menu={{
                    items: [
                        ...(isUserSubscribed
                            ? [
                                  {
                                      key: DROPDOWN_KEYS.UNSUBSCRIBE_ME,
                                      label: 'Unsubscribe Me',
                                  },
                              ]
                            : []),
                        {
                            key: DROPDOWN_KEYS.SUBSCRIBE_ME,
                            label: isUserSubscribed ? 'Manage My Subscription' : 'Subscribe Me',
                        },
                        ...(hasGroupRelationships
                            ? [
                                  {
                                      key: DROPDOWN_KEYS.SUBSCRIBE_GROUP,
                                      label: 'Manage Group Subscriptions',
                                  },
                              ]
                            : []),
                    ],
                    onClick: onClickMenuItem,
                }}
            >
                {!isSiblingMode ? (
                    <Tooltip2
                        header={() => <TooltipHeader title="Subscribers" />}
                        placement="leftBottom"
                        sections={[
                            {
                                title: 'Users',
                                titleSuffix: (
                                    <Pill label={formatNumber(numUserSubscriptions)} size="xs" clickable={false} />
                                ),
                                content: (
                                    <>
                                        <PillsContainer>
                                            {subscribedUsers && subscribedUsers.length ? (
                                                subscribedUsers.map((user) => {
                                                    const name =
                                                        user &&
                                                        entityRegistry.getDisplayName(EntityType.CorpUser, user);
                                                    const avatarUrl =
                                                        user?.editableProperties?.pictureLink || undefined;
                                                    return (
                                                        <Link
                                                            to={`${entityRegistry.getEntityUrl(
                                                                EntityType.CorpUser,
                                                                user?.urn,
                                                            )}`}
                                                        >
                                                            <CreatedByContainer>
                                                                <CustomAvatar
                                                                    size={16}
                                                                    photoUrl={avatarUrl}
                                                                    name={name}
                                                                    hideTooltip
                                                                />
                                                                <Text
                                                                    type="div"
                                                                    color="gray"
                                                                    size="xs"
                                                                    weight="semiBold"
                                                                >
                                                                    {name}
                                                                </Text>
                                                            </CreatedByContainer>
                                                        </Link>
                                                    );
                                                })
                                            ) : (
                                                <SubTitle>No users subscribed yet</SubTitle>
                                            )}
                                        </PillsContainer>
                                        {fetchMoreUsers && !!subscribedUsers.length && (
                                            <ShowMoreButton onClick={fetchMoreUsers}>View more</ShowMoreButton>
                                        )}
                                    </>
                                ),
                            },
                            {
                                title: 'Groups',
                                titleSuffix: (
                                    <Pill label={formatNumber(numGroupSubscriptions)} size="xs" clickable={false} />
                                ),
                                content: (
                                    <>
                                        <PillsContainer>
                                            {subscribedGroups && subscribedGroups.length ? (
                                                subscribedGroups.map((group) => {
                                                    const name =
                                                        group &&
                                                        entityRegistry.getDisplayName(EntityType.CorpGroup, group);
                                                    return (
                                                        <Link
                                                            to={`${entityRegistry.getEntityUrl(
                                                                EntityType.CorpGroup,
                                                                group?.urn,
                                                            )}`}
                                                        >
                                                            <CreatedByContainer>
                                                                <UsersThree size="12" color={colors.gray[1700]} />
                                                                <Text color="gray" size="xs" weight="semiBold">
                                                                    {name}
                                                                </Text>
                                                            </CreatedByContainer>
                                                        </Link>
                                                    );
                                                })
                                            ) : (
                                                <SubTitle>No groups subscribed yet</SubTitle>
                                            )}
                                        </PillsContainer>
                                        {fetchMoreGroups && subscribedGroups.length && (
                                            <ShowMoreButton onClick={fetchMoreGroups}>View more</ShowMoreButton>
                                        )}
                                    </>
                                ),
                            },
                        ]}
                    >
                        {renderSubscribeIcon()}
                    </Tooltip2>
                ) : (
                    <Tooltip2
                        title={
                            <>
                                You cannot subscribe to a group of assets. <br />
                                <br />
                                Please subscribe to the assets that this group is <b>composed of</b> by navigating to
                                them in the sidebar below.
                            </>
                        }
                        placement="leftBottom"
                    >
                        {renderSubscribeIcon()}
                    </Tooltip2>
                )}
            </Dropdown>
            <SubscriptionDrawer
                isOpen={isDrawerOpen}
                onClose={onCloseDrawer}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                setGroupUrn={setGroupUrn}
                entityUrn={entityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed={isSubscribed}
                subscription={subscription}
                canManageSubscription={canManageSubscription}
                onRefetch={refetch}
                onDeleteSubscription={deleteSubscription}
                onUpsertSubscription={handleUpsertSubscription}
            />
        </ActionMenuItem>
    );
};
