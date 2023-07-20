import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Dropdown, MenuProps, Tooltip } from 'antd';
import { StarFilled, StarOutlined } from '@ant-design/icons';
import SubscriptionDrawer from './drawer/SubscriptionDrawer';
import { useEntityData } from '../../entity/shared/EntityContext';
import SubscriptionStarTooltip from './drawer/section/SubscriptionStarTooltip';
import useSubscription from './useSubscription';
import useDeleteSubscription from './useDeleteSubscription';
import useSubscriptionSummary from './useSubscriptionSummary';
import useRelationships from './useRelationships';

const StyledStarFilled = styled(StarFilled)`
    color: ${(props) => props.theme.styles['primary-color']};
`;

const SubscribeDropdown = styled(Dropdown.Button)``;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

export default function SubscribeButtons() {
    const { urn: entityUrn, entityData, entityType } = useEntityData();
    const entityName = entityData?.name || '';
    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();

    const { hasRelationships } = useRelationships({ count: 1 });
    const { subscription, isSubscribed, refetchSubscription } = useSubscription({ isPersonal, entityUrn, groupUrn });
    const { isUserSubscribed, numUserSubscriptions, numGroupSubscriptions, groupNames, refetchSubscriptionSummary } =
        useSubscriptionSummary({ entityUrn });

    const refetch = () => {
        refetchSubscription();
        refetchSubscriptionSummary();
    };

    const deleteSubscription = useDeleteSubscription({
        subscriptionUrn: subscription?.subscriptionUrn,
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
                        ...(hasRelationships
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
                onRefetch={refetch}
                onDeleteSubscription={deleteSubscription}
            />
        </>
    );
}
