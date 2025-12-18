import { Input, Switch, Text, colors } from '@components';
import React, { useState } from 'react';
import Cron from 'react-js-cron';
import styled from 'styled-components';

import { DAILY_MIDNIGHT_CRON_INTERVAL } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/constants';

const CronText = styled.div`
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const CronInput = styled(Input)`
    max-width: 200px;
`;

const Schedule = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

const AdvancedSchedule = styled.div`
    margin-left: 20px;
`;

const ScheduleContainer = styled.div`
    .cron-builder {
        color: ${colors.gray[1800]};
        font-size: 14px;
        display: flex;
        gap: 8px;
    }
    .cron-builder-select {
        color: ${colors.gray[500]};
        font-size: 14px;

        .ant-select-selector {
            border-radius: 8px;
        }
    }

    .cron-builder-field {
        margin-bottom: 0;
    }
`;

const CronFormat = styled.div`
    background-color: ${colors.gray[1600]};
    border-radius: 4px;
    padding: 3px 6px;
    width: fit-content;
`;

interface Props {
    scheduleCronInterval: string;
    setScheduleCronInterval: React.Dispatch<React.SetStateAction<string>>;
    cronAsText: {
        text: string | undefined;
        error: boolean;
    };
}

export default function CronField({ scheduleCronInterval, setScheduleCronInterval, cronAsText }: Props) {
    const [advancedCronCheck, setAdvancedCronCheck] = useState(false);

    return (
        <ScheduleContainer>
            <Schedule>
                {advancedCronCheck ? (
                    <CronInput
                        placeholder={DAILY_MIDNIGHT_CRON_INTERVAL}
                        autoFocus
                        value={scheduleCronInterval}
                        onChange={(e) => setScheduleCronInterval(e.target.value)}
                        label=""
                    />
                ) : (
                    <Cron
                        value={scheduleCronInterval}
                        setValue={setScheduleCronInterval}
                        clearButton={false}
                        className="cron-builder"
                        leadingZero
                    />
                )}
                <AdvancedSchedule>
                    <Switch
                        label="Advanced Settings"
                        checked={advancedCronCheck}
                        onChange={(e) => setAdvancedCronCheck(e.target.checked)}
                        labelPosition="right"
                    />
                </AdvancedSchedule>
            </Schedule>
            <CronText>
                {cronAsText.error && (
                    <Text color="red" size="sm">
                        Invalid cron schedule. Cron must be of UNIX form:
                    </Text>
                )}
                {!cronAsText.text && (
                    <CronFormat>
                        <Text size="sm">minute, hour, day, month, day of week</Text>
                    </CronFormat>
                )}
                {cronAsText.text && (
                    <Text color="gray" size="sm">
                        {cronAsText.text}
                    </Text>
                )}
            </CronText>
        </ScheduleContainer>
    );
}
