import { Alert, Drawer, Typography } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components/macro';

import { ENABLE_UPSTREAM_NOTIFICATIONS } from '@app/settings/personal/notifications/constants';
import { EMAIL_SINK, NOTIFICATION_SINKS, SLACK_SINK, TEAMS_SINK } from '@app/settings/platform/types';
import { isSinkEnabled } from '@app/settings/utils';
import Footer from '@app/shared/subscribe/drawer/section/Footer';
import NotificationRecipientSection from '@app/shared/subscribe/drawer/section/NotificationRecipientSection';
import NotificationTypesSection from '@app/shared/subscribe/drawer/section/NotificationTypesSection';
import SelectGroupSection from '@app/shared/subscribe/drawer/section/SelectGroupSection';
import UpstreamSection from '@app/shared/subscribe/drawer/section/UpstreamSection';
import useDrawerActions from '@app/shared/subscribe/drawer/state/actions';
import SubscriptionDrawerProvider from '@app/shared/subscribe/drawer/state/context';
import {
    selectEmailSaveAsDefault,
    selectEmailSubscriptionChannel,
    selectIsEmailEnabled,
    selectIsSlackEnabled,
    selectIsTeamsEnabled,
    selectShouldTurnOnEmailInSettings,
    selectShouldTurnOnSlackInSettings,
    selectShouldTurnOnTeamsInSettings,
    selectSlackSaveAsDefault,
    selectSlackSubscriptionChannel,
    useDrawerSelector,
} from '@app/shared/subscribe/drawer/state/selectors';
import useDelayedKey from '@app/shared/subscribe/drawer/useDelayedKey';
import useActorSinkSettings from '@app/shared/subscribe/drawer/useSinkSettings';
import useUpsertSubscription from '@app/shared/subscribe/drawer/useUpsertSubscription';
import {
    getEmailSettingsChannel,
    getEmailSubscriptionChannel,
    getSlackSettingsChannel,
    getSlackSubscriptionChannel,
    getTeamsSettingsChannel,
    getTeamsSettingsChannelName,
    getTeamsSubscriptionChannel,
} from '@app/shared/subscribe/drawer/utils';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useAppConfig } from '@app/useAppConfig';

import { useGetLineageCountsQuery } from '@graphql/lineage.generated';
import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';
import { Assertion, DataHubPageModuleType, DataHubSubscription, EntityType, NotificationSinkType } from '@types';

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
    const { reloadByKeyType } = useReloadableContext();

    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );

    const slackSinkSupported = globallyEnabledSinks.some((sink) => sink.id === SLACK_SINK.id);
    const emailSinkSupported = globallyEnabledSinks.some((sink) => sink.id === EMAIL_SINK.id);
    const teamsSinkSupported = globallyEnabledSinks.some((sink) => sink.id === TEAMS_SINK.id);

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

    // Teams selectors
    const teamsEnabled = useDrawerSelector(selectIsTeamsEnabled);
    const shouldTurnOnTeamsInSettings = useDrawerSelector(selectShouldTurnOnTeamsInSettings);

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
    const { slackSettings, emailSettings, teamsSettings, sinkTypes, updateSinkSettings } = useActorSinkSettings({
        isPersonal,
        groupUrn,
    });

    // Slack initial configs.
    const slackSettingsChannel = getSlackSettingsChannel(isPersonal, slackSettings);
    const slackSubscriptionChannel = getSlackSubscriptionChannel(isPersonal, subscription);

    // Email initial configs.
    const emailSettingsChannel = getEmailSettingsChannel(isPersonal, emailSettings);
    const emailSubscriptionChannel = getEmailSubscriptionChannel(isPersonal, subscription);

    // Teams initial configs.
    const teamsSettingsChannel = getTeamsSettingsChannel(isPersonal, teamsSettings) || undefined;
    const teamsSettingsChannelName = getTeamsSettingsChannelName(isPersonal, teamsSettings) || undefined;
    const teamsSubscriptionChannel = getTeamsSubscriptionChannel(isPersonal, subscription) || undefined;

    useEffect(() => {
        actions.initialize({
            isPersonal,
            slackSinkEnabled: slackSinkSupported,
            emailSinkEnabled: emailSinkSupported,
            teamsSinkEnabled: teamsSinkSupported,
            entityType,
            subscription,
            forSubResource,
            slackSubscriptionChannel,
            slackSettingsChannel,
            emailSubscriptionChannel,
            emailSettingsChannel,
            teamsSubscriptionChannel,
            teamsSettingsChannel,
            teamsSettingsChannelName,
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
        teamsSettingsChannel,
        teamsSettingsChannelName,
        teamsSubscriptionChannel,
        slackSinkSupported,
        emailSinkSupported,
        teamsSinkSupported,
        subscription,
        sinkTypes,
        forSubResource,
    ]);

    const onUpdate = () => {
        upsertSubscription();
        // Reload modules
        // SubscribedAssets - update module after adding subscription
        reloadByKeyType(
            [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.SubscribedAssets)],
            3000,
        );

        const shouldUpdateNotificationSettings =
            slackSaveAsDefault ||
            emailSaveAsDefault ||
            shouldTurnOnSlackInSettings ||
            shouldTurnOnEmailInSettings ||
            shouldTurnOnTeamsInSettings;

        if (shouldUpdateNotificationSettings) {
            const newSinkTypes: NotificationSinkType[] = [];

            if (slackEnabled) {
                newSinkTypes.push(NotificationSinkType.Slack);
            }

            if (emailEnabled) {
                newSinkTypes.push(NotificationSinkType.Email);
            }

            if (teamsEnabled) {
                newSinkTypes.push(NotificationSinkType.Teams);
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
        if (isUnsubscribe) {
            onDeleteSubscription?.();
            // Reload modules
            // SubscribedAssets - update module after removing subscription
            reloadByKeyType(
                [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.SubscribedAssets)],
                3000,
            );
        }
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
