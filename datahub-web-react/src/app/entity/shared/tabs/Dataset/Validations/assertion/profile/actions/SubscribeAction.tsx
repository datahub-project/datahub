import React, { useState } from 'react';
import styled from 'styled-components';
import { BellFilled, BellOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import { ActionItem } from './ActionItem';
import useSubscription from '../../../../../../../../shared/subscribe/useSubscription';
import SubscriptionDrawer from '../../../../../../../../shared/subscribe/drawer/SubscriptionDrawer';
import { Assertion, EntityType } from '../../../../../../../../../types.generated';
import useGroupRelationships from '../../../../../../../../shared/subscribe/useGroupRelationships';
import { useEntityData } from '../../../../../../EntityContext';
import { checkIsAssetLevelAssertionSubscription } from '../../../../../../../../shared/subscribe/drawer/section/utils';

const StyledBellFilled = styled(BellFilled)`
    && {
        color: #5280e8;
    }
`;

const StyledBellOutlined = styled(BellOutlined)`
    && {
        color: #5280e8;
    }
`;

const StyledDropdown = styled(Dropdown)`
    && {
        color: #5280e8;
    }
`;

const DROPDOWN_KEYS = {
    SUBSCRIBE_ME: 'SUBSCRIBE_ME',
    SUBSCRIBE_GROUP: 'SUBSCRIBE_GROUP',
    // TODO: handle unsubscribe specific assertion
    // UNSUBSCRIBE_ME: 'UNSUBSCRIBE_ME',
} as const;

type Props = {
    assertion: Assertion;
    refetch?: () => void;
};

export const SubscribeAction = ({ assertion, refetch }: Props) => {
    const { urn, entityType, entityData } = useEntityData();
    const isEntityExists = entityType === EntityType.Dataset ? entityData?.exists : true;

    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [isPersonal, setIsPersonal] = useState(true);
    const [isDrawerForThisAssertion, setIsDrawerForThisAssertion] = useState(true);
    const [groupUrn, setGroupUrn] = useState<string>();

    const { subscription, isSubscribed, canManageSubscription, refetchSubscription } = useSubscription({
        entityUrn: urn,
        isPersonal,
        groupUrn,
        isEntityExists,
    });
    const { hasGroupRelationships } = useGroupRelationships({ count: 1 });

    const entityName = entityData?.name || '';

    const isAssetLevelAssertionSubscription = subscription?.entityChangeTypes?.some(
        checkIsAssetLevelAssertionSubscription,
    );
    const isSubscribedToThisAssertionSpecifically = subscription?.entityChangeTypes.some((changeType) =>
        changeType.filter?.includeAssertions?.includes(assertion.urn),
    );
    const isSubscribedToThisAssertionGenerally =
        isAssetLevelAssertionSubscription || isSubscribedToThisAssertionSpecifically;

    const refetchSub = () => {
        refetchSubscription?.();
        refetch?.();
    };

    const onClickMenuItem = (key: string) => {
        refetchSub();
        if (key === DROPDOWN_KEYS.SUBSCRIBE_ME) {
            setIsPersonal(true);
            setIsDrawerOpen(true);
        } else if (key === DROPDOWN_KEYS.SUBSCRIBE_GROUP) {
            setIsPersonal(false);
            setGroupUrn(undefined);
            setIsDrawerOpen(true);
        }
        // TODO: handle unsubscribe specific assertion
        // else if (key === DROPDOWN_KEYS.UNSUBSCRIBE_ME) {
        //     setIsPersonal(true);
        // }
    };

    const onCloseDrawer = () => {
        setIsDrawerOpen(false);
        setIsPersonal(true);
        setIsDrawerForThisAssertion(true);
        setGroupUrn(undefined);
        // after drawer close
        setTimeout(() => refetchSub(), 0);
    };

    const items = [
        // TODO: handle unsubscribe specific assertion
        // ...(isSubscribedToThisAssertionSpecifically
        //     ? [
        //         {
        //             key: DROPDOWN_KEYS.UNSUBSCRIBE_ME,
        //             label: <span style={{ color: '#46507b' }}>Unsubscribe Me</span>,
        //             onClick: (e) => {
        //                 e.domEvent.stopPropagation();
        //                 onClickMenuItem(DROPDOWN_KEYS.UNSUBSCRIBE_ME);
        //             },
        //         },
        //     ]
        //     : []),
        ...(isSubscribedToThisAssertionGenerally
            ? [
                  {
                      key: DROPDOWN_KEYS.SUBSCRIBE_ME,
                      label: <span style={{ color: '#46507b' }}>Manage My Subscription</span>,
                      onClick: (e) => {
                          e.domEvent.stopPropagation();
                          onClickMenuItem(DROPDOWN_KEYS.SUBSCRIBE_ME);
                      },
                  },
              ]
            : [
                  {
                      key: DROPDOWN_KEYS.SUBSCRIBE_ME,
                      label: <span style={{ color: '#46507b' }}>Subscribe Me</span>,
                      onClick: (e) => {
                          e.domEvent.stopPropagation();
                          onClickMenuItem(DROPDOWN_KEYS.SUBSCRIBE_ME);
                      },
                  },
              ]),
        ...(hasGroupRelationships
            ? [
                  {
                      key: DROPDOWN_KEYS.SUBSCRIBE_GROUP,
                      label: <span style={{ color: '#46507b' }}>Manage Group Subscriptions</span>,
                      onClick: (e) => {
                          e.domEvent.stopPropagation();
                          onClickMenuItem(DROPDOWN_KEYS.SUBSCRIBE_GROUP);
                      },
                  },
              ]
            : []),
        // TODO: remove this if not needed based on user feedback
        // ...(isAssetLevelAssertionSubscription
        //     ? [
        //         {
        //             key: DROPDOWN_KEYS.SUBSCRIBE_ME,
        //             label: (
        //                 <CardContainer>
        //                     <Title>You are currently subscribed to assertions at the asset level.</Title>
        //                     <Subtitle>
        //                         Unsubscribe from asset-level assertions to subscribe to individual assertions
        //                         instead.
        //                     </Subtitle>
        //                     <span style={{ color: '#46507b' }}>Manage My Subscription</span>
        //                 </CardContainer>
        //             ),
        //             onClick: (e) => {
        //                 e.domEvent.stopPropagation();
        //                 onClickMenuItem(DROPDOWN_KEYS.SUBSCRIBE_ME);
        //             },
        //         },
        //     ]
        //     : []),
    ];

    return (
        <div>
            <StyledDropdown menu={{ items }}>
                <span>
                    <ActionItem
                        key="subscribe-urn"
                        onClick={() => {}}
                        icon={isSubscribedToThisAssertionGenerally ? <StyledBellFilled /> : <StyledBellOutlined />}
                    />
                </span>
            </StyledDropdown>

            <SubscriptionDrawer
                isOpen={isDrawerOpen}
                onClose={onCloseDrawer}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                setGroupUrn={setGroupUrn}
                entityUrn={urn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed={isSubscribed}
                subscription={subscription}
                canManageSubscription={canManageSubscription}
                onRefetch={refetchSub}
                forSubResource={isDrawerForThisAssertion ? { assertion } : undefined}
            />
        </div>
    );
};
