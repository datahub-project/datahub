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
    // todo - audit where groupUrn and setGroupUrn are (not) passed in
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
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const {
        // todo - this isn't a form value, it's just some global context we might want to pass around
        isPersonal,
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

    const refetch = () => {
        refetchEntitySubscriptionSummary?.();
        refetchGetSubscription?.();
    };

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

    // todo - make this less slack specific somehow?
    const subscriptionChannel = getSubscriptionChannel(isPersonal, subscription);

    const initializeState = useCallback(() => {
        actions.initialize({ slackSinkEnabled, entityType, subscription, subscriptionChannel, settingsChannel });
    }, [entityType, actions, settingsChannel, slackSinkEnabled, subscription, subscriptionChannel]);

    useEffect(() => {
        initializeState();
    }, [initializeState]);

    const resetAndClose = () => {
        setTimeout(initializeState, 250);
        onClose();
    };

    const onUpdateFooter = () => {
        upsertSubscription();
        if (channel && saveAsDefault) updateSinkSettings(channel);
        resetAndClose();
    };

    const onCancelOrUnsubscribe = () => {
        if (isSubscribed) {
            onDeleteSubscription();
        }
        onClose();
        resetAndClose();
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
            onClose={resetAndClose}
            closable={false}
        >
            <SubscriptionTitleContainer>
                <SubscriptionTitle>Subscribe to {entityName}</SubscriptionTitle>
                <Button type="link" onClick={resetAndClose}>
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

const SubscriptionDrawer = ({ isPersonal, ...rest }: Props & { isPersonal: boolean }) => {
    return (
        <SubscriptionDrawerProvider isPersonal={isPersonal}>
            <SubscriptionDrawerContent {...rest} />
        </SubscriptionDrawerProvider>
    );
};

export default SubscriptionDrawer;
