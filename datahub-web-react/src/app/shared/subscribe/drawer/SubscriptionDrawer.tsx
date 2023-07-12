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
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { useGetLineageCountsQuery } from '../../../../graphql/lineage.generated';
import useEnabledSinks from '../../useEnabledSinks';

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

export default function SubscriptionDrawer({
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
}: Props) {
    const { slackSinkEnabled } = useEnabledSinks();

    const [checkedKeys, setCheckedKeys] = useState<Key[]>([]);
    const [subscribeToUpstream, setSubscribeToUpstream] = useState<boolean>(false);
    const [notificationSinkTypes, setNotificationSinkTypes] = useState<NotificationSinkType[]>([]);
    const [allowEditing, setAllowEditing] = useState<boolean>(false);

    const [saveSlackSinkAsDefault, setSaveSlackSinkAsDefault] = useState<boolean>(false);
    const [customSlackSink, setCustomSlackSink] = useState<string | undefined>(undefined);
    const [createSubscription] = useCreateSubscriptionMutation();
    const [updateSubscription] = useUpdateSubscriptionMutation();
    const subscriptionTypes = subscribeToUpstream
        ? [SubscriptionType.EntityChange, SubscriptionType.UpstreamEntityChange]
        : [SubscriptionType.EntityChange];

    // Fetch the lineage counts for the entity.
    const { data: lineageCountData } = useGetLineageCountsQuery({
        variables: {
            urn: entityUrn,
        },
    });

    const upstreamTotal = (lineageCountData?.entity as any)?.upstream?.total || 0;
    const upstreamFiltered = (lineageCountData?.entity as any)?.upstream?.filtered || 0;
    const upstreamCount = upstreamTotal - upstreamFiltered;

    useEffect(() => {
        const entityChangeTypes = subscription?.entityChangeTypes ?? getDefaultCheckedKeys(entityType);
        const sinkTypes = subscription?.notificationConfig?.sinkTypes ?? [];
        const isSubscriptionEnabled = slackSinkEnabled && sinkTypes.includes(NotificationSinkType.Slack);
        const hasUpstreamSubscription = !!subscription?.subscriptionTypes?.includes(
            SubscriptionType.UpstreamEntityChange,
        );

        setCheckedKeys(entityChangeTypes);
        setSubscribeToUpstream(hasUpstreamSubscription);
        setNotificationSinkTypes(sinkTypes);
        setAllowEditing(isSubscriptionEnabled);
    }, [
        entityType,
        slackSinkEnabled,
        subscription?.entityChangeTypes,
        subscription?.notificationConfig?.sinkTypes,
        subscription?.subscriptionTypes,
    ]);

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
    const notificationSettings: NotificationSettingsInput | undefined = customSlackSink
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

    const userHandle = userNotificationSettings?.getUserNotificationSettings?.slackSettings?.userHandle || undefined;
    const channels = groupNotificationSettings?.getGroupNotificationSettings?.slackSettings?.channels;
    const groupChannel = channels?.length ? channels[0] : undefined;
    const slackSinkDefaultValue = isPersonal ? userHandle : groupChannel;

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
        if (saveSlackSinkAsDefault) {
            updateSinkSetting(customSlackSink || '');
        }
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
                    allowEditing={allowEditing}
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
                    {/* todo - this is disabled until we have a proper implementation */}
                    {false && (
                        <UpstreamSection
                            entityUrn={entityUrn}
                            entityType={entityType}
                            subscribeToUpstream={subscribeToUpstream}
                            setSubscribeToUpstream={setSubscribeToUpstream}
                            upstreamCount={upstreamCount}
                        />
                    )}
                    <NotificationRecipientSection
                        isPersonal={isPersonal}
                        slackSinkDefaultValue={slackSinkDefaultValue}
                        setCustomSlackSink={setCustomSlackSink}
                        notificationSinkTypes={notificationSinkTypes}
                        setNotificationSinkTypes={setNotificationSinkTypes}
                        allowEditing={allowEditing}
                        setAllowEditing={setAllowEditing}
                        saveSlackSinkAsDefault={saveSlackSinkAsDefault}
                        setSaveSlackSinkAsDefault={setSaveSlackSinkAsDefault}
                    />
                </>
            )}
        </SubscribeDrawer>
    );
}
