import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { Alert, Drawer, Typography } from 'antd';
import NotificationTypesSection from './section/NotificationTypesSection';
import UpstreamSection from './section/UpstreamSection';
import NotificationRecipientSection from './section/NotificationRecipientSection';
import Footer from './section/Footer';
import SelectGroupSection from './section/SelectGroupSection';
import { Assertion, DataHubSubscription, EntityType, NotificationSinkType } from '../../../../types.generated';
import {
    getEmailSettingsChannel,
    getEmailSubscriptionChannel,
    getSlackSettingsChannel,
    getSlackSubscriptionChannel,
} from './utils';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';
import { useGetLineageCountsQuery } from '../../../../graphql/lineage.generated';
import { EMAIL_SINK, NOTIFICATION_SINKS, SLACK_SINK } from '../../../settings/platform/types';
import { isSinkEnabled } from '../../../settings/utils';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../settings/personal/notifications/constants';
import SubscriptionDrawerProvider from './state/context';
import useDrawerActions from './state/actions';
import useActorSinkSettings from './useSinkSettings';
import useUpsertSubscription from './useUpsertSubscription';
import useDelayedKey from './useDelayedKey';
import {
    selectIsSlackEnabled,
    selectShouldTurnOnSlackInSettings,
    selectSlackSubscriptionChannel,
    selectSlackSaveAsDefault,
    useDrawerSelector,
    selectShouldTurnOnEmailInSettings,
    selectIsEmailEnabled,
    selectEmailSaveAsDefault,
    selectEmailSubscriptionChannel,
} from './state/selectors';
import { useAppConfig } from '../../../useAppConfig';

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
    font-family: 'Mulish', sans-serif;
    font-size: 24px;
    line-height: 32px;
    font-weight: 400;
`;

const StyledAlert = styled(Alert)`
    margin-top: 16px;
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
    forSubResource?: {
        assertion?: Assertion;
    };
    onRefetch?: () => void;
    onUpsertSubscription?: () => void;
    onDeleteSubscription?: () => void;
}

// TODO: Decide whether the abstraction used within this component is really warranted.
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
    forSubResource,
    onRefetch,
    onUpsertSubscription,
    onDeleteSubscription,
}: Props) => {
    const { config } = useAppConfig();
    const { data: globalSettings } = useGetGlobalSettingsQuery();

    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );

    const slackSinkSupported = globallyEnabledSinks.some((sink) => sink.id === SLACK_SINK.id);
    const emailSinkSupported = globallyEnabledSinks.some((sink) => sink.id === EMAIL_SINK.id);

    // Slack selectors
    const slackChannel = useDrawerSelector(selectSlackSubscriptionChannel);
    const slackSaveAsDefault = useDrawerSelector(selectSlackSaveAsDefault);
    const slackEnabled = useDrawerSelector(selectIsSlackEnabled);
    const shouldTurnOnSlackInSettings = useDrawerSelector(selectShouldTurnOnSlackInSettings);

    // Email selectors
    const emailChannel = useDrawerSelector(selectEmailSubscriptionChannel);
    const emailSaveAsDefault = useDrawerSelector(selectEmailSaveAsDefault);
    const emailEnabled = useDrawerSelector(selectIsEmailEnabled);
    const shouldTurnOnEmailInSettings = useDrawerSelector(selectShouldTurnOnEmailInSettings);

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
        forSubResource,
    });

    const showBottomDrawerSection = isPersonal || (groupUrn && canManageSubscription);

    // Retrieve the default settings for the user for each sink type.
    const { slackSettings, emailSettings, sinkTypes, updateSinkSettings } = useActorSinkSettings({
        isPersonal,
        groupUrn,
    });

    // Slack initial configs.
    const slackSettingsChannel = getSlackSettingsChannel(isPersonal, slackSettings);
    const slackSubscriptionChannel = getSlackSubscriptionChannel(isPersonal, subscription);

    // Email initial configs.
    const emailSettingsChannel = getEmailSettingsChannel(isPersonal, emailSettings);
    const emailSubscriptionChannel = getEmailSubscriptionChannel(isPersonal, subscription);

    useEffect(() => {
        actions.initialize({
            isPersonal,
            slackSinkEnabled: slackSinkSupported,
            emailSinkEnabled: emailSinkSupported,
            entityType,
            subscription,
            forSubResource,
            slackSubscriptionChannel,
            slackSettingsChannel,
            emailSubscriptionChannel,
            emailSettingsChannel,
            settingsSinkTypes: sinkTypes,
        });
    }, [
        actions,
        entityType,
        isPersonal,
        slackSettingsChannel,
        slackSubscriptionChannel,
        emailSettingsChannel,
        emailSubscriptionChannel,
        slackSinkSupported,
        emailSinkSupported,
        subscription,
        sinkTypes,
        forSubResource,
    ]);

    const onUpdate = () => {
        upsertSubscription();

        const shouldUpdateNotificationSettings =
            slackSaveAsDefault || emailSaveAsDefault || shouldTurnOnSlackInSettings || shouldTurnOnEmailInSettings;

        if (shouldUpdateNotificationSettings) {
            const newSinkTypes: NotificationSinkType[] = [];

            if (slackEnabled) {
                newSinkTypes.push(NotificationSinkType.Slack);
            }

            if (emailEnabled) {
                newSinkTypes.push(NotificationSinkType.Email);
            }

            // New slack settings
            let newSlackSettings = isPersonal
                ? { userHandle: slackSettingsChannel }
                : { channels: slackSettingsChannel ? [slackSettingsChannel] : [] };

            if (slackSaveAsDefault) {
                newSlackSettings = isPersonal
                    ? { userHandle: slackChannel }
                    : { channels: slackChannel ? [slackChannel] : [] };
            }

            // New email settings
            let newEmailSettings = emailSettingsChannel ? { email: emailSettingsChannel } : undefined;

            if (emailSaveAsDefault) {
                newEmailSettings = emailChannel ? { email: emailChannel } : undefined;
            }

            updateSinkSettings({
                slackSettings: newSlackSettings,
                emailSettings: newEmailSettings,
                sinkTypes: newSinkTypes || [],
            });
        }

        onClose();
    };

    const onCancelOrUnsubscribe = (isUnsubscribe: boolean) => {
        if (isUnsubscribe) onDeleteSubscription?.();
        onClose();
    };

    return (
        <SubscribeDrawer
            width={512}
            footer={
                <Footer
                    canManageSubscription={canManageSubscription}
                    isSubscribed={isSubscribed}
                    forSubResource={forSubResource}
                    onCancelOrUnsubscribe={onCancelOrUnsubscribe}
                    onUpdate={onUpdate}
                />
            }
            open={isOpen}
            onClose={onClose}
            closable={false}
        >
            <SubscriptionTitleContainer>
                {/* TODO: enter assertion name here (derrive it from parameters if needed) */}
                <SubscriptionTitle>
                    Subscribe to {forSubResource?.assertion ? 'assertion...' : entityName}
                </SubscriptionTitle>
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
                    <NotificationTypesSection
                        entityUrn={entityUrn}
                        entityType={entityType}
                        subscription={subscription}
                        forSubResource={forSubResource}
                        onClose={onClose}
                    />
                    {ENABLE_UPSTREAM_NOTIFICATIONS && (
                        <UpstreamSection entityUrn={entityUrn} entityType={entityType} upstreamCount={upstreamCount} />
                    )}
                    <NotificationRecipientSection isPersonal={isPersonal} />
                </>
            )}
        </SubscribeDrawer>
    );
};

const SubscriptionDrawer = (props: Props) => {
    const key = useDelayedKey({ condition: !props.isOpen });

    const handleClick = (e) => {
        e.stopPropagation();
    };
    /* eslint-disable jsx-a11y/click-events-have-key-events */
    return (
        <SubscriptionDrawerProvider key={key}>
            {/* Disabling no-static-element-interactions because we are deliberately using a div with an onClick handler to prevent propagation. */}
            {/* eslint-disable-next-line jsx-a11y/no-static-element-interactions */}
            <div onClick={handleClick}>
                <SubscriptionDrawerContent {...props} />
            </div>
        </SubscriptionDrawerProvider>
    );
};

export default SubscriptionDrawer;
