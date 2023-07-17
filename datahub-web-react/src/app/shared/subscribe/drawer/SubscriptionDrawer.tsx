import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { Button, Drawer, Typography } from 'antd';
import { CloseCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import NotificationTypesSection from './section/NotificationTypesSection';
import UpstreamSection from './section/UpstreamSection';
import NotificationRecipientSection from './section/NotificationRecipientSection';
import Footer from './section/Footer';
import SelectGroupSection from './section/SelectGroupSection';
import {
    updateGroupNotificationSettingsFunction,
    updateUserNotificationSettingsFunction,
} from '../../../settings/personal/notifications/utils';
import {
    DataHubSubscription,
    EntityChangeType,
    EntityType,
    NotificationSettingsInput,
    SubscriptionType,
} from '../../../../types.generated';
import {
    createSubscriptionFunction,
    getEntityChangeTypesFromCheckedKeys,
    getSubscriptionChannel,
    getUserSettingsChannel,
    updateSubscriptionFunction,
} from './utils';
import {
    useCreateSubscriptionMutation,
    useUpdateSubscriptionMutation,
} from '../../../../graphql/subscriptions.generated';
import {
    useGetGlobalSettingsQuery,
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { useGetLineageCountsQuery } from '../../../../graphql/lineage.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../settings/platform/types';
import { isSinkEnabled } from '../../../settings/utils';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../settings/personal/notifications/constants';
import SubscriptionFormProvider, { useFormDispatchContext, useFormStateContext } from './form/context';

const SubscribeDrawer = styled(Drawer)``;

const SubscriptionTitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
`;

const SubscriptionTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 24px;
    line-height: 32px;
    font-weight: 400;
`;

interface Props {
    isOpen: boolean;
    onClose: () => void;
    groupUrn?: string;
    setGroupUrn?: (groupUrn: string | undefined) => void;
    entityUrn: string;
    entityName: string;
    entityType: EntityType;
    isSubscribed: boolean;
    subscription: DataHubSubscription | undefined;
    refetchGetSubscription?: () => void;
    refetchEntitySubscriptionSummary?: () => void;
    onDeleteSubscription: () => void;
}

const SubscriptionDrawerContent = ({
    isOpen,
    onClose,
    groupUrn,
    setGroupUrn,
    entityUrn,
    entityName,
    entityType,
    isSubscribed,
    subscription,
    refetchGetSubscription,
    refetchEntitySubscriptionSummary,
    onDeleteSubscription,
}: Props) => {
    // todo - put this into the reducer somehow
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const {
        // todo - this isn't a form value, it's just some global context we might want to pass around
        isPersonal,
        checkedKeys,
        subscribeToUpstream,
        notificationSinkTypes,
        slack: {
            subscription: { channel, saveAsDefault },
        },
    } = useFormStateContext();
    const dispatch = useFormDispatchContext();

    const [createSubscription] = useCreateSubscriptionMutation();
    const [updateSubscription] = useUpdateSubscriptionMutation();
    const subscriptionTypes = subscribeToUpstream
        ? [SubscriptionType.EntityChange, SubscriptionType.UpstreamEntityChange]
        : [SubscriptionType.EntityChange];

    // Skipping until we want to enable upstreams
    const { data: lineageCountData } = useGetLineageCountsQuery({
        skip: true,
        variables: {
            urn: entityUrn,
        },
    });

    const upstreamTotal = (lineageCountData?.entity as any)?.upstream?.total || 0;
    const upstreamFiltered = (lineageCountData?.entity as any)?.upstream?.filtered || 0;
    const upstreamCount = upstreamTotal - upstreamFiltered;

    useEffect(() => {
        if (isPersonal) {
            setGroupUrn?.(undefined);
        }
    }, [isPersonal, setGroupUrn]);

    const refetch = () => {
        refetchEntitySubscriptionSummary?.();
        refetchGetSubscription?.();
    };

    const entityChangeTypes: EntityChangeType[] = getEntityChangeTypesFromCheckedKeys(checkedKeys);

    const notificationSettings: NotificationSettingsInput | undefined =
        channel && !saveAsDefault
            ? {
                  slackSettings: {
                      userHandle: isPersonal ? channel : undefined,
                      channels: isPersonal ? undefined : [channel],
                  },
              }
            : undefined;

    const onCreateSubscription = () => {
        createSubscriptionFunction(
            createSubscription,
            refetch,
            groupUrn || undefined,
            entityUrn,
            subscriptionTypes,
            entityChangeTypes,
            notificationSinkTypes,
            notificationSettings,
        );
    };

    const onUpdateSubscription = () => {
        updateSubscriptionFunction(
            updateSubscription,
            refetch,
            subscription,
            subscriptionTypes,
            entityChangeTypes,
            notificationSinkTypes,
            notificationSettings,
        );
    };

    const onUpsertSubscription = isSubscribed ? onUpdateSubscription : onCreateSubscription;
    const showBottomDrawerSection = isPersonal || groupUrn;

    const { data: userNotificationSettings, refetch: refetchUserNotificationSettings } =
        useGetUserNotificationSettingsQuery({ skip: !isPersonal });
    const { data: groupNotificationSettings, refetch: refetchGroupNotificationSettings } =
        useGetGroupNotificationSettingsQuery({
            skip: isPersonal || !groupUrn,
            variables: { input: { groupUrn: groupUrn || '' } },
        });

    const subscriptionChannel = getSubscriptionChannel(isPersonal, subscription);
    const settingsChannel = getUserSettingsChannel(isPersonal, userNotificationSettings, groupNotificationSettings);

    useEffect(() => {
        dispatch({
            type: 'initialize',
            payload: { slackSinkEnabled, entityType, subscription, subscriptionChannel, settingsChannel },
        });
    }, [dispatch, entityType, settingsChannel, slackSinkEnabled, subscription, subscriptionChannel]);

    const [updateUserNotificationSettings] = useUpdateUserNotificationSettingsMutation();
    const [updateGroupNotificationSettings] = useUpdateGroupNotificationSettingsMutation();
    const onUpdateUserNotificationSettings = (newUserHandle: string) => {
        updateUserNotificationSettingsFunction(
            newUserHandle,
            updateUserNotificationSettings,
            refetchUserNotificationSettings,
        );
    };

    const onUpdateGroupNotificationSettings = (newGroupChannel: string) => {
        updateGroupNotificationSettingsFunction(
            groupUrn || '',
            newGroupChannel,
            updateGroupNotificationSettings,
            refetchGroupNotificationSettings,
        );
    };

    const updateSinkSetting = isPersonal ? onUpdateUserNotificationSettings : onUpdateGroupNotificationSettings;

    // Final update functions
    const onUpdateFooter = () => {
        onUpsertSubscription();
        if (channel && saveAsDefault) updateSinkSetting(channel);
        onClose();
    };

    const onCancelOrUnsubscribe = () => {
        if (isSubscribed) {
            onDeleteSubscription();
        }
        onClose();
    };

    return (
        <SubscribeDrawer
            width={512}
            footer={
                <Footer
                    isSubscribed={isSubscribed}
                    onCancelOrUnsubscribe={onCancelOrUnsubscribe}
                    onUpdate={onUpdateFooter}
                />
            }
            open={isOpen}
            onClose={onClose}
            closable={false}
        >
            <SubscriptionTitleContainer>
                <SubscriptionTitle>Subscribe to {entityName}</SubscriptionTitle>
                <Button type="link" onClick={onClose}>
                    <CloseCircleOutlined style={{ color: ANTD_GRAY[10] }} />
                </Button>
            </SubscriptionTitleContainer>
            {!isPersonal && <SelectGroupSection groupUrn={groupUrn} setGroupUrn={setGroupUrn} />}
            {showBottomDrawerSection && (
                <>
                    <NotificationTypesSection />
                    {ENABLE_UPSTREAM_NOTIFICATIONS && (
                        <UpstreamSection entityUrn={entityUrn} entityType={entityType} upstreamCount={upstreamCount} />
                    )}
                    {/* todo - push settings value into context? */}
                    {/* maybe we don't need it if it's just some kinda fallback value or idk maybe we don't even need the subscriptionChannel on the form? */}
                    <NotificationRecipientSection />
                </>
            )}
        </SubscribeDrawer>
    );
};

const SubscriptionDrawer = ({ isPersonal, ...rest }: Props & { isPersonal: boolean }) => {
    return (
        <SubscriptionFormProvider isPersonal={isPersonal}>
            <SubscriptionDrawerContent {...rest} />
        </SubscriptionFormProvider>
    );
};

export default SubscriptionDrawer;
