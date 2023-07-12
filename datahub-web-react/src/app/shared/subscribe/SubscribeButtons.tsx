import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Dropdown, MenuProps, Tooltip } from 'antd';
import { StarFilled, StarOutlined } from '@ant-design/icons';
import SubscriptionDrawer from './drawer/SubscriptionDrawer';
import {
    useDeleteSubscriptionMutation,
    useGetEntitySubscriptionSummaryQuery,
    useGetSubscriptionQuery,
} from '../../../graphql/subscriptions.generated';
import { useEntityData } from '../../entity/shared/EntityContext';
import SubscriptionStarTooltip from './drawer/section/SubscriptionStarTooltip';
import { getGroupName } from '../../settings/personal/utils';
import { CorpGroup, DataHubSubscription } from '../../../types.generated';
import { deleteSubscriptionFunction } from './drawer/utils';

const StyledStarFilled = styled(StarFilled)`
    color: ${(props) => props.theme.styles['primary-color']};
`;

const SubscribeDropdown = styled(Dropdown.Button)``;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
};

export default function SubscribeButtons() {
    const { urn: entityUrn, entityData, entityType } = useEntityData();
    const entityName = entityData?.name || '';
    const [drawerIsOpen, setDrawerIsOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string | undefined>(undefined);
    const [subscription, setSubscription] = useState<DataHubSubscription | undefined>(undefined);
    const [isSubscribed, setIsSubscribed] = useState(false);
    const { data: getSubscriptionData, refetch: refetchGetSubscription } = useGetSubscriptionQuery({
        variables: {
            input: {
                entityUrn,
                groupUrn: groupUrn || undefined,
            },
        },
    });
    const { data: entitySubscriptionSummaryData, refetch: refetchEntitySubscriptionSummary } =
        useGetEntitySubscriptionSummaryQuery({
            variables: {
                input: {
                    entityUrn,
                },
            },
        });
    const isUserSubscribed = entitySubscriptionSummaryData?.getEntitySubscriptionSummary?.isUserSubscribed || false;
    const numUserSubscriptions = entitySubscriptionSummaryData?.getEntitySubscriptionSummary.numUserSubscriptions || 0;
    // Maxes out at 100 by default.
    const numGroupSubscriptions =
        entitySubscriptionSummaryData?.getEntitySubscriptionSummary.numGroupSubscriptions || 0;
    const groupNames: string[] =
        (entitySubscriptionSummaryData?.getEntitySubscriptionSummary.topGroups
            .map((group) => getGroupName(group as CorpGroup))
            .filter((name) => !!name) as string[]) || [];

    const [deleteSubscription] = useDeleteSubscriptionMutation();
    const refetchDeleteSubscription = () => {
        refetchGetSubscription();
        refetchEntitySubscriptionSummary();
    };

    const onDeleteSubscription = () => {
        if (subscription && subscription.subscriptionUrn) {
            deleteSubscriptionFunction(subscription.subscriptionUrn, deleteSubscription, refetchDeleteSubscription);
        }
    };

    const handleMenuClick: MenuProps['onClick'] = (e) => {
        const key = e.key as string;
        if (key === DROPDOWN_KEYS.SUBSCRIBE_ME) {
            setIsPersonal(true);
            setDrawerIsOpen(true);
        } else if (key === DROPDOWN_KEYS.SUBSCRIBE_GROUP) {
            setIsPersonal(false);
            setGroupUrn(undefined);
            setSubscription(undefined);
            setIsSubscribed(false);
            setDrawerIsOpen(true);
        } else if (key === DROPDOWN_KEYS.UNSUBSCRIBE_ME) {
            setIsPersonal(true);
            onDeleteSubscription();
        }
    };

    let items: MenuProps['items'] = [
        {
            key: DROPDOWN_KEYS.SUBSCRIBE_ME,
            label: isSubscribed ? 'Manage My Subscription' : 'Subscribe Me',
        },
        {
            key: DROPDOWN_KEYS.SUBSCRIBE_GROUP,
            label: 'Manage Group Subscriptions',
        },
    ];
    if (isSubscribed) {
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

    useEffect(() => {
        setSubscription(getSubscriptionData?.getSubscription || undefined);
        setIsSubscribed(!!getSubscriptionData?.getSubscription);
    }, [getSubscriptionData]);

    return (
        <>
            <SubscribeDropdown
                menu={menuProps}
                buttonsRender={([leftButton, rightButton]) => [
                    <Tooltip
                        title={
                            <SubscriptionStarTooltip
                                isSubscribed={isSubscribed}
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
                onClose={() => setDrawerIsOpen(false)}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                setGroupUrn={setGroupUrn}
                entityUrn={entityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed={isSubscribed}
                subscription={subscription}
                refetchGetSubscription={refetchGetSubscription}
                refetchEntitySubscriptionSummary={refetchEntitySubscriptionSummary}
                onDeleteSubscription={onDeleteSubscription}
            />
        </>
    );
}
