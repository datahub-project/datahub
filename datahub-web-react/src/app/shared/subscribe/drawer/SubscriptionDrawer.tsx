import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { Alert, Button, Drawer, Typography } from 'antd';
import { CloseCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import NotificationTypesSection from './section/NotificationTypesSection';
import UpstreamSection from './section/UpstreamSection';
import NotificationRecipientSection from './section/NotificationRecipientSection';
import Footer from './section/Footer';
import SelectGroupSection from './section/SelectGroupSection';
import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../types.generated';
import { getSubscriptionChannel } from './utils';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';
import { useGetLineageCountsQuery } from '../../../../graphql/lineage.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../settings/platform/types';
import { isSinkEnabled } from '../../../settings/utils';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../settings/personal/notifications/constants';
import SubscriptionDrawerProvider from './state/context';
import useDrawerActions from './state/actions';
import useSinkSettings from './useSinkSettings';
import useUpsertSubscription from './useUpsertSubscription';
import useDelayedKey from './useDelayedKey';
import {
    selectIsSlackEnabled,
    selectShouldTurnOnSlackInSettings,
    selectSubscriptionSlackChannel,
    selectSlackSaveAsDefault,
    useDrawerSelector,
} from './state/selectors';

const SubscribeDrawer = styled(Drawer)`
    .ant-drawer-body {
        padding: 16px 24px;
    }
`;

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

const StyledAlert = styled(Alert)`
    margin-top: 16px;
`;

const CancelButtonWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const StyledButton = styled(Button)`
    padding: 0 4px;
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
    canManageSubscription?: boolean | null;
    subscription?: DataHubSubscription;
    onRefetch?: () => void;
    onUpsertSubscription?: () => void;
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
    canManageSubscription,
    subscription,
    onRefetch,
    onUpsertSubscription,
    onDeleteSubscription,
}: Props) => {
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const channel = useDrawerSelector(selectSubscriptionSlackChannel);
    const saveAsDefault = useDrawerSelector(selectSlackSaveAsDefault);
    const slackEnabled = useDrawerSelector(selectIsSlackEnabled);
    const shouldTurnOnSlackInSettings = useDrawerSelector(selectShouldTurnOnSlackInSettings);
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
        entityType,
        isSubscribed,
        groupUrn,
        subscription,
        onCreateSuccess: () => onUpsertSubscription?.(),
        onRefetch,
    });

    const showBottomDrawerSection = isPersonal || (groupUrn && canManageSubscription);

    const { settingsChannel, sinkTypes, updateSinkSettings } = useSinkSettings({
        isPersonal,
        groupUrn,
    });

    useEffect(() => {
        actions.initialize({
            isPersonal,
            slackSinkEnabled,
            entityType,
            subscription,
            subscriptionChannel: getSubscriptionChannel(isPersonal, subscription),
            settingsChannel,
            settingsSinkTypes: sinkTypes,
        });
    }, [actions, entityType, isPersonal, settingsChannel, slackSinkEnabled, subscription, sinkTypes]);

    const onUpdate = () => {
        upsertSubscription();
        if (channel && saveAsDefault) {
            updateSinkSettings({ text: channel, sinkTypes: slackEnabled ? [NotificationSinkType.Slack] : [] });
        } else if (shouldTurnOnSlackInSettings) {
            updateSinkSettings({
                text: settingsChannel as string,
                sinkTypes: slackEnabled ? [NotificationSinkType.Slack] : [],
            });
        }
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
                <Footer
                    canManageSubscription={canManageSubscription}
                    isSubscribed={isSubscribed}
                    onCancelOrUnsubscribe={onCancelOrUnsubscribe}
                    onUpdate={onUpdate}
                />
            }
            open={isOpen}
            onClose={onClose}
            closable={false}
        >
            <CancelButtonWrapper>
                <StyledButton type="link" onClick={onClose}>
                    <CloseCircleOutlined style={{ color: ANTD_GRAY[10] }} />
                </StyledButton>
            </CancelButtonWrapper>
            <SubscriptionTitleContainer>
                <SubscriptionTitle>Subscribe to {entityName}</SubscriptionTitle>
            </SubscriptionTitleContainer>
            {!isPersonal && <SelectGroupSection groupUrn={groupUrn} setGroupUrn={setGroupUrn} />}
            {canManageSubscription === false && (
                <StyledAlert
                    type="warning"
                    message="You do not have permissions to manage this group's subscriptions. Please contact your DataHub Administrator."
                    showIcon
                />
            )}
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
