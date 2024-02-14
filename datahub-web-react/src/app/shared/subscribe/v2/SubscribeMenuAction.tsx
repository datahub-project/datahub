import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Dropdown, MenuProps } from 'antd';
import { BellFilled, BellOutlined } from '@ant-design/icons';
import SubscriptionDrawer from '../drawer/SubscriptionDrawer';
import useSubscription from '../useSubscription';
import useDeleteSubscription from '../useDeleteSubscription';
import useSubscriptionSummary from '../useSubscriptionSummary';
import useGroupRelationships from '../useGroupRelationships';
import { ActionMenuItem } from '../../../entityV2/shared/EntityDropdown/styledComponents';
import { useEntityData, useMutationUrn } from '../../../entityV2/shared/EntityContext';

const StyledBellFilled = styled(BellFilled)`
    && {
        height: 100%;
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
    }
`;

const StyledBellOutlined = styled(BellOutlined)`
    && {
        height: 100%;
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
    }
`;

const HoverDropdown = styled(Dropdown)``;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

export default function SubscribeMenuAction() {
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

    const { isUserSubscribed, setIsUserSubscribed, refetchSubscriptionSummary } = useSubscriptionSummary({
        entityUrn: primaryEntityUrn,
    });

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
            <HoverDropdown
                data-testid="entity-menu-subscribe-button"
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
                {isUserSubscribed ? <StyledBellFilled /> : <StyledBellOutlined />}
            </HoverDropdown>
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
        </ActionMenuItem>
    );
}
