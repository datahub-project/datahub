import React, { ReactElement, ReactNode } from 'react';
import { Steps, Tag } from 'antd';
import { blue } from '@ant-design/colors';
import { DataRunEntity, stateToColorMapping, stateToStepMapping } from '../interfaces';
import {
    formatUTCDateString,
    getRunState,
    isFinishedSLAType,
    isStartedSLAType,
    getSLAText,
    convertSecsToHumanReadable,
} from '../functions';

// get state tag with color from stateToColorMapping
function getStateTag(state: string, date: string): ReactElement {
    return (
        <>
            <Tag color={stateToColorMapping[state] ?? blue.primary}>{state.toLowerCase()}</Tag> {date}
        </>
    );
}

/**
 * display steps for latest run
 * @param run latest run to examine
 */
export const LatestRunSteps = ({ run }: { run: DataRunEntity }) => {
    const latestRunState = getRunState(run);
    const currentStep: number = stateToStepMapping[latestRunState] || 0;

    // get start date description
    const latestStartTimeText = `Started at ${formatUTCDateString(run.execution.startDate, true)}`;
    // get end date description with state, if not yet finished, use current UTC time
    const latestEndTimeText: ReactNode = run.execution?.endDate
        ? getStateTag(latestRunState, ` at ${formatUTCDateString(run.execution.endDate, true)}`)
        : getStateTag(latestRunState, ` as of ${formatUTCDateString(new Date().getTime(), true)}`);

    // get progress step description
    const latestRunProgressText = run.execution?.endDate
        ? `Ran for: ${convertSecsToHumanReadable(
              (new Date(run.execution.endDate).getTime() - new Date(run.execution.startDate).getTime()) / 1000,
          )}`
        : `Running for: ${convertSecsToHumanReadable(
              (new Date().getTime() - new Date(run.execution.startDate).getTime()) / 1000,
          )}`;

    return (
        <Steps current={currentStep}>
            <Steps.Step title="Scheduled" description={formatUTCDateString(run.execution.logicalDate)} />
            <Steps.Step title={latestStartTimeText} description={getSLAText(run, isStartedSLAType)} />
            <Steps.Step title="In Progress" description={latestRunProgressText} />
            <Steps.Step title={latestEndTimeText} description={getSLAText(run, isFinishedSLAType)} />
        </Steps>
    );
};
