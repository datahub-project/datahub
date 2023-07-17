import React, { Key, useEffect, useState } from 'react';
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
    NotificationSinkType,
    SubscriptionType,
} from '../../../../types.generated';
import {
    createSubscriptionFunction,
    getDefaultCheckedKeys,
    getEntityChangeTypesFromCheckedKeys,
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
import SubscriptionFormProvider, { useFormActionContext } from './form/context';

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
    isPersonal: boolean;
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
    isPersonal,
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
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const [checkedKeys, setCheckedKeys] = useState<Key[]>([]);
    const [subscribeToUpstream, setSubscribeToUpstream] = useState<boolean>(false);
    const [notificationSinkTypes, setNotificationSinkTypes] = useState<NotificationSinkType[]>([]);
    const dispatch = useFormActionContext();

    const [saveSlackSinkAsDefault, setSaveSlackSinkAsDefault] = useState<boolean>(false);
    const [customSlackSink, setCustomSlackSink] = useState<string>();
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

    const subUserHandle = subscription?.notificationConfig?.notificationSettings?.slackSettings.userHandle || undefined;
    const subChannels = subscription?.notificationConfig?.notificationSettings?.slackSettings?.channels;
    const subGroupChannel = subChannels?.length ? subChannels[0] : undefined;
    const slackSinkSubscriptionValue = isPersonal ? subUserHandle : subGroupChannel;

    useEffect(() => {
        // todo - what if we just have an "initialize" dispatch that can setup the state based on the subscription we have here?
        const entityChangeTypes = subscription?.entityChangeTypes ?? getDefaultCheckedKeys(entityType);
        const sinkTypes = subscription?.notificationConfig?.sinkTypes ?? [];
        // todo - this is the slack specific one, maybe our reducer can handle the logic of enabling only when all children are enabled?
        const hasUpstreamSubscription =
            ENABLE_UPSTREAM_NOTIFICATIONS &&
            !!subscription?.subscriptionTypes?.includes(SubscriptionType.UpstreamEntityChange);

        setCheckedKeys(entityChangeTypes);
        setSubscribeToUpstream(hasUpstreamSubscription);
        setNotificationSinkTypes(sinkTypes);
        setCustomSlackSink(slackSinkSubscriptionValue);

        dispatch({ type: 'initialize', payload: { slackSinkEnabled, entityType, subscription } });
    }, [slackSinkSubscriptionValue, entityType, slackSinkEnabled, subscription, dispatch]);

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
        customSlackSink && !saveSlackSinkAsDefault
            ? {
                  slackSettings: {
                      userHandle: isPersonal ? customSlackSink : undefined,
                      channels: isPersonal ? undefined : [customSlackSink],
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

    // Section for updating notification settings
    const { data: userNotificationSettings, refetch: refetchUserNotificationSettings } =
        useGetUserNotificationSettingsQuery({ skip: !isPersonal });
    const { data: groupNotificationSettings, refetch: refetchGroupNotificationSettings } =
        useGetGroupNotificationSettingsQuery({
            skip: isPersonal || !groupUrn,
            variables: { input: { groupUrn: groupUrn || '' } },
        });

    const settingsUserHandle =
        userNotificationSettings?.getUserNotificationSettings?.slackSettings?.userHandle || undefined;
    const settingsChannels = groupNotificationSettings?.getGroupNotificationSettings?.slackSettings?.channels;
    const settingsGroupChannel = settingsChannels?.length ? settingsChannels[0] : undefined;
    const slackSinkSettingsValue = isPersonal ? settingsUserHandle : settingsGroupChannel;

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
        if (customSlackSink && saveSlackSinkAsDefault) updateSinkSetting(customSlackSink);
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
                    <NotificationTypesSection checkedKeys={checkedKeys} setCheckedKeys={setCheckedKeys} />
                    {ENABLE_UPSTREAM_NOTIFICATIONS && (
                        <UpstreamSection
                            entityUrn={entityUrn}
                            entityType={entityType}
                            subscribeToUpstream={subscribeToUpstream}
                            setSubscribeToUpstream={setSubscribeToUpstream}
                            upstreamCount={upstreamCount}
                        />
                    )}
                    <NotificationRecipientSection
                        customSlackSink={customSlackSink}
                        isPersonal={isPersonal}
                        slackSinkSubscriptionValue={slackSinkSubscriptionValue}
                        slackSinkSettingsValue={slackSinkSettingsValue}
                        notificationSinkTypes={notificationSinkTypes}
                        setCustomSlackSink={setCustomSlackSink}
                        setNotificationSinkTypes={setNotificationSinkTypes}
                        setSaveSlackSinkAsDefault={setSaveSlackSinkAsDefault}
                    />
                </>
            )}
        </SubscribeDrawer>
    );
};

const SubscriptionDrawer = (props: Props) => {
    return (
        <SubscriptionFormProvider>
            <SubscriptionDrawerContent {...props} />
        </SubscriptionFormProvider>
    );
};

export default SubscriptionDrawer;
