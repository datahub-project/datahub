import { Icon, Switch, Text } from '@components';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { TimezoneSelect } from '@app/ingestV2/source/builder/TimezoneSelect';
import CronField from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/syncScheduleSection/CronField';
import { lowerFirstLetter } from '@app/shared/textUtil';
import { cronToString } from '@utils/cronstrue';

const FieldsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const SwitchLabel = styled.div`
    display: flex;
    gap: 2px;
`;

const WarningContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const TimezoneContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export type ScheduleFieldsProps = {
    scheduleEnabled: boolean;
    onScheduleEnabledChange: (enabled: boolean) => void;
    cronInterval: string;
    onCronIntervalChange: (interval: string) => void;
    timezone: string;
    onTimezoneChange: (timezone: string) => void;
    /** Overrides the ingestion-flavoured switch label ("Keep metadata current by…"). */
    switchLabel?: string;
    /** Overrides the ingestion-flavoured warning shown when no schedule is set. */
    noScheduleWarning?: string;
};

/**
 * The controlled schedule editor shared by every ingestion-source form: the
 * on/off switch, the cron builder (with advanced free-text mode) and the
 * timezone picker. Owns no state — the host decides how the values persist.
 */
export function ScheduleFields({
    scheduleEnabled,
    onScheduleEnabledChange,
    cronInterval,
    onCronIntervalChange,
    timezone,
    onTimezoneChange,
    switchLabel,
    noScheduleWarning,
}: ScheduleFieldsProps) {
    const { t } = useTranslation('ingestion.sourceBuilder');

    const cronAsText = useMemo(() => {
        if (cronInterval) {
            try {
                return {
                    text: t('multiStep.schedule.runs', {
                        schedule: lowerFirstLetter(cronToString(cronInterval)),
                    }),
                    error: false,
                };
            } catch (e) {
                return {
                    text: undefined,
                    error: true,
                };
            }
        }
        return {
            text: undefined,
            error: false,
        };
    }, [cronInterval, t]);

    return (
        <FieldsContainer>
            <SwitchLabel>
                <Text size="sm" weight="bold" color="gray" colorLevel={600}>
                    {t('multiStep.schedule.runOnSchedule')}
                </Text>
                <Text size="sm" weight="bold" color="gray" colorLevel={1700}>
                    {t('multiStep.schedule.recommended')}
                </Text>
            </SwitchLabel>
            <Switch
                label={switchLabel ?? t('multiStep.schedule.switchLabel')}
                checked={scheduleEnabled}
                onChange={(e) => onScheduleEnabledChange(e.target.checked)}
                labelPosition="right"
                data-testid="schedule-enabled-switch"
            />
            {!scheduleEnabled && (
                <WarningContainer>
                    <Icon icon={Warning} color="yellow" colorLevel={1000} size="md" />
                    <Text color="yellow" colorLevel={1000} size="sm">
                        {noScheduleWarning ?? t('multiStep.schedule.noScheduleWarning')}
                    </Text>
                </WarningContainer>
            )}
            <CronField
                scheduleCronInterval={cronInterval}
                setScheduleCronInterval={onCronIntervalChange}
                cronAsText={cronAsText}
            />
            <TimezoneContainer>
                <Text color="gray">{t('multiStep.schedule.chooseTimezone')}</Text>
                <TimezoneSelect value={timezone} onChange={onTimezoneChange} />
            </TimezoneContainer>
        </FieldsContainer>
    );
}
