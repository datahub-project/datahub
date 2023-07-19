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

const StyledStarFilled = styled(StarFilled)`
    color: ${(props) => props.theme.styles['primary-color']};
`;

const SubscribeDropdown = styled(Dropdown.Button)``;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

// todo - open subscribed group  drawer -> shows all 3 btns by mistake
// - toggling the drawer, switches some global state state in here permanently
export default function SubscribeButtons() {
    const { urn: entityUrn, entityData, entityType } = useEntityData();
    const entityName = entityData?.name || '';
    const [drawerIsOpen, setDrawerIsOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();

    // This is a little bad feeling, because we're hoping our mutation will run before this...
    // callMutation() and setIsPersonal(true) ----- sometime later we actually take the mutation props?
    const onCloseDrawer = () => {
        setDrawerIsOpen(false);
        setIsPersonal(true);
        setGroupUrn(undefined);
    };

    const { subscription, isSubscribed, refetchSubscription } = useSubscription({ isPersonal, entityUrn, groupUrn });
    const { isUserSubscribed, numUserSubscriptions, numGroupSubscriptions, groupNames, refetchSubscriptionSummary } =
        useSubscriptionSummary({ entityUrn });

    const refetch = () => {
        refetchSubscription();
        refetchSubscriptionSummary();
    };

    // todo - we need to be careful here
    const deleteSubscription = useDeleteSubscription({
        subscriptionUrn: subscription?.subscriptionUrn,
        onSuccess: refetch,
    });

    const handleMenuClick: MenuProps['onClick'] = ({ key }) => {
        if (key === DROPDOWN_KEYS.SUBSCRIBE_ME) {
            setIsPersonal(true);
            setDrawerIsOpen(true);
        } else if (key === DROPDOWN_KEYS.SUBSCRIBE_GROUP) {
            setIsPersonal(false);
            setGroupUrn(undefined);
            setDrawerIsOpen(true);
        } else if (key === DROPDOWN_KEYS.UNSUBSCRIBE_ME) {
            setIsPersonal(true);
            deleteSubscription();
        }
    };

    let items: MenuProps['items'] = [
        {
            key: DROPDOWN_KEYS.SUBSCRIBE_ME,
            label: isUserSubscribed ? 'Manage My Subscription' : 'Subscribe Me',
        },
        {
            key: DROPDOWN_KEYS.SUBSCRIBE_GROUP,
            label: 'Manage Group Subscriptions',
        },
    ];
    if (isUserSubscribed) {
        items = [
            {
                key: DROPDOWN_KEYS.UNSUBSCRIBE_ME,
                label: 'Unsubscribe Me',
            },
            ...items,
        ];
    }

    const menuProps = {
        items,
        onClick: handleMenuClick,
    };

    return (
        <>
            <SubscribeDropdown
                menu={menuProps}
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
                    React.cloneElement(rightButton as React.ReactElement<any, string>),
                ]}
                onClick={() => setDrawerIsOpen(true)}
            >
                {isUserSubscribed ? <StyledStarFilled /> : <StarOutlined />}
            </SubscribeDropdown>
            <SubscriptionDrawer
                isOpen={drawerIsOpen}
                onClose={onCloseDrawer}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                setGroupUrn={setGroupUrn}
                entityUrn={entityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed={isSubscribed}
                subscription={subscription}
                refetch={refetch}
                onDeleteSubscription={deleteSubscription}
            />
        </>
    );
}
