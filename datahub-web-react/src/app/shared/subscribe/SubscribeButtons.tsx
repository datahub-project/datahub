import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Dropdown, MenuProps, Tooltip } from 'antd';
import { CaretDownFilled, StarFilled, StarOutlined } from '@ant-design/icons';
import SubscriptionDrawer from './drawer/SubscriptionDrawer';
import { useEntityData, useMutationUrn } from '../../entity/shared/EntityContext';
import SubscriptionStarTooltip from './drawer/section/SubscriptionStarTooltip';
import useSubscription from './useSubscription';
import useDeleteSubscription from './useDeleteSubscription';
import useSubscriptionSummary from './useSubscriptionSummary';
import useGroupRelationships from './useGroupRelationships';
import { ENTITY_PROFILE_SUBSCRIPTION_ID } from '../../onboarding/config/EntityProfileOnboardingConfig';

const StyledStarFilled = styled(StarFilled)`
    color: ${(props) => props.theme.styles['primary-color']};
`;

const SubscribeDropdown = styled(Dropdown.Button)``;

const ArrowDown = styled(CaretDownFilled)`
    svg {
        height: 14px;
        width: 14px;
    }
`;

const ArrowDownWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

export default function SubscribeButtons() {
    const { entityData, entityType } = useEntityData();
    const primaryEntityUrn = useMutationUrn();
    const entityName = entityData?.name || '';
    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();

    const { hasGroupRelationships } = useGroupRelationships({ count: 1 });
    const { subscription, isSubscribed, canManageSubscription, refetchSubscription } = useSubscription({
        isPersonal,
        entityUrn: primaryEntityUrn,
        groupUrn,
    });

    const {
        isUserSubscribed,
        numUserSubscriptions,
        numGroupSubscriptions,
        groupNames,
        setIsUserSubscribed,
        refetchSubscriptionSummary,
    } = useSubscriptionSummary({ entityUrn: primaryEntityUrn });

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

    const onClickStar = () => {
        setIsDrawerOpen(true);
        setIsPersonal(true);
        setGroupUrn(undefined);
    };

    const onCloseDrawer = () => {
        setIsDrawerOpen(false);
        setIsPersonal(true);
        setGroupUrn(undefined);
    };

    return (
        <>
            <span id={ENTITY_PROFILE_SUBSCRIPTION_ID}>
                <SubscribeDropdown
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
                    icon={
                        <ArrowDownWrapper>
                            <ArrowDown data-testid="subscription-dropdown" />
                        </ArrowDownWrapper>
                    }
                    buttonsRender={([leftButton, rightButton]) => [
                        <Tooltip
                            title={
                                <SubscriptionStarTooltip
                                    isUserSubscribed={isUserSubscribed}
                                    numUserSubscriptions={numUserSubscriptions}
                                    numGroupSubscriptions={numGroupSubscriptions}
                                    groupNames={groupNames}
                                />
                            }
                            placement="left"
                            color="#262626"
                        >
                            {leftButton}
                        </Tooltip>,
                        rightButton,
                    ]}
                    onClick={onClickStar}
                >
                    {isUserSubscribed ? <StyledStarFilled /> : <StarOutlined />}
                </SubscribeDropdown>
            </span>
            <SubscriptionDrawer
                isOpen={isDrawerOpen}
                onClose={onCloseDrawer}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                setGroupUrn={setGroupUrn}
                entityUrn={primaryEntityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed={isSubscribed}
                subscription={subscription}
                canManageSubscription={canManageSubscription}
                onRefetch={refetch}
                onDeleteSubscription={deleteSubscription}
                onUpsertSubscription={handleUpsertSubscription}
            />
        </>
    );
}
