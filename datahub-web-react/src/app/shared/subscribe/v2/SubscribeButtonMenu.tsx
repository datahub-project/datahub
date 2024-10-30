import React, { useEffect, useState } from 'react';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import SubscriptionDrawer from '../drawer/SubscriptionDrawer';
import useSubscription from '../useSubscription';
import useDeleteSubscription from '../useDeleteSubscription';
import useGroupRelationships from '../useGroupRelationships';
import { StyledMenuItem } from '../../share/v2/styledComponents';
import { EntityType } from '../../../../types.generated';
import { GenericEntityProperties } from '../../../entity/shared/types';
import useSubscriptionSummary from '../useSubscriptionSummary';

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

interface Props {
    setIsFetchingSubscriptionSummary: (isFetchingSubscriptionSummary: boolean) => void;
    setIsSubscribed: (isSubscribed: boolean) => void;
    entityUrn: string;
    entityData: GenericEntityProperties | null;
    entityType: EntityType;
}

export default function SubscribeButtonMenu({
    setIsFetchingSubscriptionSummary,
    setIsSubscribed,
    entityUrn,
    entityData,
    entityType,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const entityName = entityRegistry.getDisplayName(entityType, entityData);
    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();

    const { hasGroupRelationships } = useGroupRelationships({ count: 1 });
    const { subscription, isSubscribed, canManageSubscription, refetchSubscription } = useSubscription({
        isPersonal,
        entityUrn,
        groupUrn,
    });

    const { isUserSubscribed, setIsUserSubscribed, refetchSubscriptionSummary, isFetchingSubscriptionSummary } =
        useSubscriptionSummary({
            entityUrn,
        });

    useEffect(() => {
        setIsFetchingSubscriptionSummary(isFetchingSubscriptionSummary);
        setIsSubscribed(isUserSubscribed);
    }, [isFetchingSubscriptionSummary, isUserSubscribed, setIsFetchingSubscriptionSummary, setIsSubscribed]);

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

    const onClickMenuItem = (key: string) => {
        if (key === DROPDOWN_KEYS.SUBSCRIBE_ME) {
            refetch();
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

    const items = [
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
    ];

    return (
        <>
            {!isFetchingSubscriptionSummary &&
                items.map((item) => (
                    <StyledMenuItem key={item.key} onClick={() => onClickMenuItem(item.key)}>
                        {item.label}
                    </StyledMenuItem>
                ))}
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
        </>
    );
}
