import { Switch } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { NOTIFICATION_SINKS, TEAMS_SINK } from '@app/settings/platform/types';
import { isSinkEnabled } from '@app/settings/utils';
import TeamsDisabledMessage from '@app/shared/subscribe/drawer/section/TeamsNotificationRecipientsSection/TeamsDisabledMessage';
// import TeamsSaveAsDefault from '@app/shared/subscribe/drawer/section/TeamsNotificationRecipientsSection/TeamsSaveAsDefault';
import TeamsSearchInterface from '@app/shared/subscribe/drawer/section/TeamsNotificationRecipientsSection/TeamsSearchInterface';
import TeamsWarningAlert from '@app/shared/subscribe/drawer/section/TeamsNotificationRecipientsSection/TeamsWarningAlert';
import useDrawerActions from '@app/shared/subscribe/drawer/state/actions';
import {
    selectIsPersonal,
    selectShouldShowUpdateTeamsSettingsWarning,
    selectTeams,
    selectTeamsSettingsChannel,
    selectTeamsSettingsChannelName,
    useDrawerSelector,
} from '@app/shared/subscribe/drawer/state/selectors';
import { ChannelSelections, TeamsState } from '@app/shared/subscribe/drawer/state/types';
import { TeamsSearchResult, useTeamsSearch } from '@app/shared/subscribe/drawer/teams-search-client';
import { useAppConfig } from '@app/useAppConfig';
import { Text } from '@src/alchemy-components';
import { useUserContext } from '@src/app/context/useUserContext';

import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';

const NotificationSwitchContainer = styled.div`
    margin-top: 16px;
    display: flex;
    flex-direction: column;
    justify-content: center;
`;

const StyledSwitch = styled(Switch)`
    margin-right: 8px;
`;

const SwitchWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const SinkTypeText = styled(Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
`;

export default function TeamsNotificationRecipientsSection() {
    const { config } = useAppConfig();

    const actions = useDrawerActions();
    const me = useUserContext();
    const { clearResults } = useTeamsSearch();

    const teams = useDrawerSelector(selectTeams) as TeamsState;
    const isPersonal = useDrawerSelector(selectIsPersonal);
    const settingsTeamsChannel = useDrawerSelector(selectTeamsSettingsChannel);
    const settingsTeamsChannelName = useDrawerSelector(selectTeamsSettingsChannelName);
    const shouldShowUpdateTeamsSettingsWarning = useDrawerSelector(selectShouldShowUpdateTeamsSettingsWarning);

    const [selectedOption, setSelectedOption] = useState<TeamsSearchResult | null>(null);
    const [isSubscriptionChannelSelected] = [teams.channelSelection === ChannelSelections.SUBSCRIPTION];
    const [, setTeamsChannelName] = useState('');
    const [previousTeamsChannelName, setPreviousTeamsChannelName] = useState({ isUpdated: false, text: '' });

    const { data: globalSettings } = useGetGlobalSettingsQuery();

    // Initialize selectedOption from persisted state and sync when it changes
    useEffect(() => {
        setSelectedOption(teams.selectedResult ?? null);
    }, [teams.selectedResult]);

    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );

    const teamsSinkSupported = globallyEnabledSinks.some((sink) => sink.id === TEAMS_SINK.id);

    const updateTeamsInState = useCallback(
        (channelName: string, selectedResult?: TeamsSearchResult, saveAsDefault = false) => {
            const teamsData: TeamsState = {
                enabled: true,
                channelSelection: saveAsDefault ? 'SETTINGS' : 'SUBSCRIPTION',
                subscription: {
                    saveAsDefault,
                    channel: saveAsDefault ? undefined : channelName,
                },
                selectedResult: selectedResult || null,
            };
            actions.setWholeTeamsObject(teamsData);
        },
        [actions],
    );

    useEffect(() => {
        if (teams.enabled && isSubscriptionChannelSelected) {
            // Only update if there's actually a change to avoid infinite loops
            const currentChannel = teams.subscription.channel || '';
            const currentSaveAsDefault = teams.subscription.saveAsDefault;
            const currentSelectedResult = teams.selectedResult;

            // Check if we need to update anything
            const needsUpdate =
                teams.channelSelection !== 'SUBSCRIPTION' ||
                teams.subscription.channel !== currentChannel ||
                teams.subscription.saveAsDefault !== currentSaveAsDefault ||
                teams.selectedResult !== currentSelectedResult;

            if (needsUpdate) {
                updateTeamsInState(currentChannel, currentSelectedResult || undefined, currentSaveAsDefault);
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [!!isSubscriptionChannelSelected, teams.enabled]);

    const onChangeTeamsSwitch = (checked: boolean) => {
        actions.setTeamsEnabled(checked);
        if (checked && !settingsTeamsChannel) {
            // Handle case where Teams is enabled but no settings channel
        }
    };

    useEffect(() => {
        if (!settingsTeamsChannel && isPersonal) {
            // No placeholder needed for select
        } else if (settingsTeamsChannel && isPersonal) {
            // Update placeholder for existing user
        } else {
            // Channel mode
        }
        // Use subscription channel if in SUBSCRIPTION mode, otherwise use settings channel
        // For display, we need the channel NAME, not the ID
        const channelToDisplay =
            teams.channelSelection === 'SUBSCRIPTION'
                ? teams.subscription.channel
                : settingsTeamsChannelName || settingsTeamsChannel; // Use name if available, fallback to ID

        setTeamsChannelName(channelToDisplay ?? '');
        if (!previousTeamsChannelName.isUpdated && settingsTeamsChannel) {
            setPreviousTeamsChannelName({ isUpdated: true, text: settingsTeamsChannel ?? '' });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [settingsTeamsChannel]);

    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;

    const handleSelectResult = useCallback(
        (result: TeamsSearchResult) => {
            setSelectedOption(result);
            updateTeamsInState(result.id, result, teams.subscription.saveAsDefault);
            clearResults();
        },
        [updateTeamsInState, teams.subscription.saveAsDefault, clearResults],
    );

    const handleSwitchToPersonalUser = useCallback(() => {
        // Use settings mode (personal user) instead of subscription mode
        const teamsData: TeamsState = {
            enabled: true,
            channelSelection: 'SETTINGS', // This is the key fix!
            subscription: {
                saveAsDefault: false,
                channel: undefined, // Clear the channel
            },
            selectedResult: null, // Clear the selected result
        };
        actions.setWholeTeamsObject(teamsData);
    }, [actions]);

    // TODO: Comment out unused function for save as default functionality
    // const handleSaveAsDefaultToggle = (checked: boolean) => {
    //     updateTeamsInState(
    //         teams.subscription.channel || '',
    //         teams.selectedResult || selectedOption || undefined,
    //         !!checked,
    //     );
    // };

    const renderTeamsSink = () => {
        if (!teamsSinkSupported) {
            return <TeamsDisabledMessage isAdminAccess={isAdminAccess} />;
        }

        return (
            <TeamsSearchInterface
                onSelectResult={handleSelectResult}
                selectedResult={selectedOption}
                teamsSinkSupported={teamsSinkSupported}
                onSwitchToPersonalUser={handleSwitchToPersonalUser}
            />
        );
    };

    return (
        <NotificationSwitchContainer>
            <SwitchWrapper>
                <StyledSwitch
                    disabled={!teamsSinkSupported}
                    size="small"
                    checked={teams.enabled}
                    onChange={onChangeTeamsSwitch}
                />
                <SinkTypeText>Teams</SinkTypeText>
            </SwitchWrapper>
            <TeamsWarningAlert show={shouldShowUpdateTeamsSettingsWarning} />
            {teams.enabled && renderTeamsSink()}
            {/* TODO: Whether we really need this functionality 
            {teams.enabled && teamsSinkSupported && (
                <TeamsSaveAsDefault
                    isChecked={teams.subscription.saveAsDefault}
                    onToggle={handleSaveAsDefaultToggle}
                    settingsTeamsChannel={settingsTeamsChannel}
                    selectedOption={selectedOption}
                    selectedResult={teams.selectedResult}
                    subscriptionChannel={teams.subscription.channel}
                />
            )}
            */}
        </NotificationSwitchContainer>
    );
}
