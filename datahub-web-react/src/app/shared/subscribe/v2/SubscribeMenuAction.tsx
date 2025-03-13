import { ENTITY_PROFILE_V2_SUBSCRIPTION_ID } from '@app/onboarding/configV2/EntityProfileOnboardingConfig';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useIsSeparateSiblingsMode } from '@src/app/entityV2/shared/useIsSeparateSiblingsMode';
import styled from 'styled-components/macro';
import { Dropdown, Tabs } from 'antd';
import { Button, colors, Icon, Pill, Text } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { Bell, UsersThree } from '@phosphor-icons/react';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
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
import { useSiblingOptionsForSubscriptions } from './utils';

const { TabPane } = Tabs;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

const DropdownContent = styled.div`
    min-width: 300px;
    background: white;
    border-radius: 8px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08);
    padding: 12px;
`;

const Header = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
`;

const Section = styled.div`
    margin-bottom: 16px;
`;

const SectionTitle = styled.div`
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 8px;
    color: ${colors.gray[1700]};
`;

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

const StyledTabs = styled(Tabs)`
    .ant-tabs-nav {
        margin-bottom: 16px;
    }

    .ant-tabs-tab {
        padding: 8px 12px;
        margin: 0;
        font-size: 14px;
    }
    .ant-tabs-ink-bar {
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    .ant-tabs-tab-active {
        border-radius: 4px;

        .ant-tabs-tab-btn {
            color: ${REDESIGN_COLORS.TITLE_PURPLE} !important;
        }
    }
`;

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    border: 1px red solid;
    padding: 10px;
`;

const ActionWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-top: 12px;
`;

export const SubscribeMenuAction = () => {
    const entityRegistry = useEntityRegistry();
    const { entityData, urn, entityType } = useEntityData();

    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();

    const { hasGroupRelationships } = useGroupRelationships({ count: 1 });

    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const isSiblingMode = (entityData?.siblingsSearch?.total && !isSeparateSiblingsMode) || false;
    const platformOptions = useSiblingOptionsForSubscriptions(entityData, urn, entityType, isSiblingMode);

    const [activeTab, setActiveTab] = useState(urn);
    const activeEntityIndex = platformOptions.findIndex((option) => option.urn === activeTab);
    const activeEntityUrn = platformOptions[activeEntityIndex].urn;
    const activeEntityName = platformOptions[activeEntityIndex].title;
    const activeEntityType = platformOptions[activeEntityIndex].entityType;

    const isEntityExists = activeEntityType === EntityType.Dataset ? entityData?.exists : true;

    const { subscription, isSubscribed, canManageSubscription, refetchSubscription } = useSubscription({
        isPersonal,
        entityUrn: activeEntityUrn,
        groupUrn,
        isEntityExists,
    });
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
    } = useSubscriptionSummary({ entityUrn: activeEntityUrn, isEntityExists });

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
    const onClickMenuItem = ({ key }: { key: (typeof DROPDOWN_KEYS)[keyof typeof DROPDOWN_KEYS] }) => {
        setIsDropdownOpen(false);
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

    const onDropdownOpenChange = (open: boolean) => {
        setIsDropdownOpen(open);
        if (open) {
            refetch();
        }
    };
    const onCloseDrawer = () => {
        setIsDrawerOpen(false);
        setIsPersonal(true);
        setGroupUrn(undefined);
    };

    const dropdownContent = (
        <DropdownContent>
            {/* Header */}
            <Header>Subscribers</Header>
            {/* Tabs */}
            <StyledTabs activeKey={activeTab} onChange={setActiveTab}>
                {platformOptions.map((option) => (
                    <TabPane tab={option.title} key={option.urn}>
                        <Section>
                            {/* Users */}
                            <SectionTitle>
                                Users <Pill label={formatNumber(numUserSubscriptions)} size="xs" clickable={false} />
                            </SectionTitle>
                            <PillsContainer>
                                {subscribedUsers && subscribedUsers.length > 0 ? (
                                    subscribedUsers.map((user) => (
                                        <Link
                                            to={`${entityRegistry.getEntityUrl(EntityType.CorpUser, user?.urn)}`}
                                            key={user?.urn}
                                        >
                                            <Pill
                                                label={user?.properties?.displayName || ''}
                                                variant="outline"
                                                customIconRenderer={() => (
                                                    <CustomAvatar
                                                        photoUrl={user?.editableProperties?.pictureLink || ''}
                                                        name={user?.properties?.displayName || ''}
                                                        size={16}
                                                        hideTooltip
                                                    />
                                                )}
                                            />
                                        </Link>
                                    ))
                                ) : (
                                    <Text>No users subscribed yet</Text>
                                )}
                            </PillsContainer>
                            {fetchMoreUsers && subscribedUsers.length > 0 && (
                                <ShowMoreButton onClick={fetchMoreUsers}>View more</ShowMoreButton>
                            )}
                        </Section>
                        {/* Groups */}
                        <Section>
                            <SectionTitle>
                                Groups <Pill label={formatNumber(numGroupSubscriptions)} size="xs" clickable={false} />
                            </SectionTitle>
                            <PillsContainer>
                                {subscribedGroups && subscribedGroups.length > 0 ? (
                                    subscribedGroups.map((group) => (
                                        <Link
                                            to={`${entityRegistry.getEntityUrl(EntityType.CorpGroup, group?.urn)}`}
                                            key={group?.urn}
                                        >
                                            <Pill
                                                label={entityRegistry.getDisplayName(EntityType.CorpGroup, group)}
                                                variant="outline"
                                                customIconRenderer={() => <UsersThree size={16} />}
                                            />
                                        </Link>
                                    ))
                                ) : (
                                    <Text>No groups subscribed yet</Text>
                                )}
                            </PillsContainer>
                            {fetchMoreGroups && subscribedGroups.length > 0 && (
                                <ShowMoreButton onClick={fetchMoreGroups}>View more</ShowMoreButton>
                            )}
                        </Section>
                    </TabPane>
                ))}
            </StyledTabs>
            {/* Subscribe Button */}
            <ActionWrapper>
                {/* Manage group */}
                {hasGroupRelationships && (
                    <Button
                        onClick={() => onClickMenuItem({ key: DROPDOWN_KEYS.SUBSCRIBE_GROUP })}
                        style={{ marginRight: 12 }}
                        variant="text"
                        data-testid="manage-for-groups-button"
                    >
                        Manage for Groups
                    </Button>
                )}
                {/* Manage my subscription */}
                <Button
                    onClick={() => onClickMenuItem({ key: DROPDOWN_KEYS.SUBSCRIBE_ME })}
                    data-testid="manage-my-subscription-button"
                >
                    {isUserSubscribed ? 'Subscribed' : 'Subscribe me'}
                    {isUserSubscribed ? <Icon source="phosphor" icon="Pencil" size="sm" /> : null}
                </Button>
            </ActionWrapper>
        </DropdownContent>
    );

    return (
        <ActionMenuItem key="subscribe" id={ENTITY_PROFILE_V2_SUBSCRIPTION_ID}>
            <Dropdown
                overlay={dropdownContent}
                open={isDropdownOpen && !isDrawerOpen}
                onOpenChange={onDropdownOpenChange}
            >
                {renderSubscribeIcon()}
            </Dropdown>
            <SubscriptionDrawer
                isOpen={isDrawerOpen}
                onClose={onCloseDrawer}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                setGroupUrn={setGroupUrn}
                entityUrn={activeEntityUrn}
                entityName={activeEntityName}
                entityType={activeEntityType || entityType} // we can fallback to the entity type of the primary entity
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
