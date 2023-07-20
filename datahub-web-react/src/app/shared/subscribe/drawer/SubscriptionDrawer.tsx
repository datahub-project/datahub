import React, { useCallback, useEffect } from 'react';
import styled from 'styled-components/macro';
import { Button, Drawer, Typography } from 'antd';
import { CloseCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import NotificationTypesSection from './section/NotificationTypesSection';
import UpstreamSection from './section/UpstreamSection';
import NotificationRecipientSection from './section/NotificationRecipientSection';
import Footer from './section/Footer';
import SelectGroupSection from './section/SelectGroupSection';
import { DataHubSubscription, EntityType } from '../../../../types.generated';
import { getSubscriptionChannel } from './utils';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';
import { useGetLineageCountsQuery } from '../../../../graphql/lineage.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../settings/platform/types';
import { isSinkEnabled } from '../../../settings/utils';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../settings/personal/notifications/constants';
import SubscriptionDrawerProvider, { useDrawerState } from './state/context';
import useDrawerActions from './state/actions';
import useSinkSettings from './useSinkSettings';
import useUpsertSubscription from './useUpsertSubscription';
import useDelayedKey from './useDelayedKey';

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
    setGroupUrn?: (groupUrn?: string) => void;
    entityUrn: string;
    entityName: string;
    entityType: EntityType;
    isSubscribed: boolean;
    subscription?: DataHubSubscription;
    refetch?: () => void;
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
    refetch,
    onDeleteSubscription,
}: Props) => {
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const {
        slack: {
            subscription: { channel, saveAsDefault },
        },
    } = useDrawerState();
    const actions = useDrawerActions();

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

    const upsertSubscription = useUpsertSubscription({
        entityUrn,
        isSubscribed,
        groupUrn,
        subscription,
        onSuccess: refetch,
    });

    const showBottomDrawerSection = isPersonal || groupUrn;

    const { settingsChannel, updateSinkSettings } = useSinkSettings({
        isPersonal,
        groupUrn,
    });

    const initializeState = useCallback(() => {
        actions.initialize({
            isPersonal,
            slackSinkEnabled,
            entityType,
            subscription,
            subscriptionChannel: getSubscriptionChannel(isPersonal, subscription),
            settingsChannel,
        });
    }, [actions, entityType, isPersonal, settingsChannel, slackSinkEnabled, subscription]);

    useEffect(() => {
        initializeState();
    }, [initializeState]);

    const onUpdate = () => {
        upsertSubscription();
        if (channel && saveAsDefault) updateSinkSettings(channel);
        onClose();
    };

    const onCancelOrUnsubscribe = () => {
        if (isSubscribed) onDeleteSubscription();
        onClose();
    };

    return (
        <SubscribeDrawer
            width={512}
            footer={
                <Footer isSubscribed={isSubscribed} onCancelOrUnsubscribe={onCancelOrUnsubscribe} onUpdate={onUpdate} />
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
                    <NotificationRecipientSection />
                </>
            )}
        </SubscribeDrawer>
    );
};

const SubscriptionDrawer = (props: Props) => {
    const key = useDelayedKey({ condition: !props.isOpen });

    return (
        <SubscriptionDrawerProvider key={key}>
            <SubscriptionDrawerContent {...props} />
        </SubscriptionDrawerProvider>
    );
};

export default SubscriptionDrawer;
