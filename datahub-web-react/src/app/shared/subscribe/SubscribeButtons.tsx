import { CaretDownFilled, StarFilled, StarOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Dropdown, MenuProps } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData, useMutationUrn } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { ENTITY_PROFILE_SUBSCRIPTION_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import SubscriptionDrawer from '@app/shared/subscribe/drawer/SubscriptionDrawer';
import SubscriptionStarTooltip from '@app/shared/subscribe/drawer/section/SubscriptionStarTooltip';
import useDeleteSubscription from '@app/shared/subscribe/useDeleteSubscription';
import useGroupRelationships from '@app/shared/subscribe/useGroupRelationships';
import useSubscription from '@app/shared/subscribe/useSubscription';
import useSubscriptionSummary from '@app/shared/subscribe/useSubscriptionSummary';

import { EntityType } from '@types';

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
    const isEntityExists = entityType === EntityType.Dataset ? entityData?.exists : true;

    const { hasGroupRelationships } = useGroupRelationships({ count: 1 });
    const { subscription, isSubscribed, canManageSubscription, refetchSubscription } = useSubscription({
        isPersonal,
        entityUrn: primaryEntityUrn,
        groupUrn,
        isEntityExists,
    });

    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const isSiblingMode = (entityData?.siblingsSearch?.total && !isSeparateSiblingsMode) || false;

    const {
        isUserSubscribed,
        numUserSubscriptions,
        numGroupSubscriptions,
        groupNames,
        setIsUserSubscribed,
        refetchSubscriptionSummary,
    } = useSubscriptionSummary({ entityUrn: primaryEntityUrn, isEntityExists });

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
                    disabled={isSiblingMode}
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
                                !isSiblingMode ? (
                                    <SubscriptionStarTooltip
                                        isUserSubscribed={isUserSubscribed}
                                        numUserSubscriptions={numUserSubscriptions}
                                        numGroupSubscriptions={numGroupSubscriptions}
                                        groupNames={groupNames}
                                    />
                                ) : (
                                    <>
                                        You cannot subscribe to a group of assets. <br />
                                        <br />
                                        Please subscribe to the assets that this group is <b>Composed Of</b> by
                                        navigating to them in the sidebar below.
                                    </>
                                )
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
                    {isUserSubscribed && !isSiblingMode ? <StyledStarFilled /> : <StarOutlined />}
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
