import React, { ReactElement } from 'react';
import moment from 'moment-timezone';
import { renderToString } from 'react-dom/server';
import { blue, grey, gold, red } from '@ant-design/colors';
import { DataRunEntity, SLAMissData, SLATypes } from './interfaces';

export function convertSecsToHumanReadable(seconds: number, truncateToMins = false) {
    const oriSeconds = seconds;
    const floatingPart = oriSeconds - Math.floor(oriSeconds);

    let secondsFloor = Math.floor(seconds);

    const secondsPerHour = 60 * 60;
    const secondsPerMinute = 60;

    const hours = Math.floor(secondsFloor / secondsPerHour);
    secondsFloor -= hours * secondsPerHour;

    const minutes = Math.floor(secondsFloor / secondsPerMinute);
    secondsFloor -= minutes * secondsPerMinute;

    let readableFormat = '';
    if (hours > 0) {
        readableFormat += `${hours}Hours `;
    }
    if (minutes > 0) {
        readableFormat += `${minutes}Min `;
    }
    if (truncateToMins) {
        return readableFormat === '' ? '0Mins' : readableFormat;
    }
    if (secondsFloor + floatingPart > 0) {
        if (Math.floor(oriSeconds) === oriSeconds) {
            readableFormat += `${secondsFloor}Sec `;
        } else {
            secondsFloor += floatingPart;
            readableFormat += `${secondsFloor.toFixed(2)}Sec`;
        }
    }
    return readableFormat;
}

// format utc date value into string
export function formatUTCDateString(date: number, includeSeconds = false): string {
    let formatString = 'YYYY-MM-DD ';
    const utcMoment = moment.utc(date);
    if (utcMoment.hour() !== 0 || utcMoment.minute() !== 0) {
        formatString += 'HH:mm';
        formatString = includeSeconds ? `${formatString}:ss` : formatString;
    }
    return moment.utc(date).format(formatString);
}

// get string state of run
export function getRunState(run): string {
    if (run && run?.state && run?.state[0]?.result?.resultType) {
        return run?.state[0].result.resultType;
    }
    return 'RUNNING';
}

// return if SLA is WARN level
export function isWarnLevelSLA(sla: SLAMissData): boolean {
    return [SLATypes.warnFinishedBy, SLATypes.warnStartedBy].includes(sla.slaType);
}

// return if SLA is ERROR level
export function isErrorLevelSLA(sla: SLAMissData): boolean {
    return [SLATypes.errorFinishedBy, SLATypes.errorStartedBy].includes(sla.slaType);
}

// return if SLA is on start date
export function isStartedSLAType(sla: SLAMissData): boolean {
    return [SLATypes.errorStartedBy, SLATypes.warnStartedBy].includes(sla.slaType);
}

// return if SLA is on end date
export function isFinishedSLAType(sla: SLAMissData): boolean {
    return [SLATypes.errorFinishedBy, SLATypes.warnFinishedBy].includes(sla.slaType);
}

// gather all SLA misses' data on run entity, including whether it missed SLA, SLA type, how much it missed by...etc
export function getSLAMissData(run: DataRunEntity): SLAMissData[] {
    const state = getRunState(run);
    const slaMissData: SLAMissData[] = [];

    if (run?.slaInfo && run.slaInfo.slaDefined === 'true') {
        const startDate = new Date(run.execution.startDate);
        const execDate = new Date(run.execution.logicalDate);
        // get end date, if no end date is set, use current time
        let endDate = new Date();
        if (run.execution?.endDate) {
            endDate = new Date(run.execution.endDate);
        }

        // prioritize error SLA misses
        if (run.slaInfo?.errorStartedBy) {
            const target = new Date(execDate.getTime());
            target.setSeconds(new Date(execDate.getTime()).getSeconds() + run.slaInfo.errorStartedBy);
            let missedBy = 0;
            let timeRemaining = 0;
            let missedSLA = false;
            if (startDate > target) {
                missedBy = (startDate.getTime() - target.getTime()) / 1000;
                missedSLA = true;
            } else {
                timeRemaining = (target.getTime() - startDate.getTime()) / 1000;
            }
            slaMissData.push({
                slaType: SLATypes.errorStartedBy,
                sla: run.slaInfo.errorStartedBy,
                missedBy,
                timeRemaining,
                missedSLA,
                state,
            } as SLAMissData);
        }

        if (run.slaInfo?.errorFinishedBy) {
            const target = new Date(execDate.getTime());
            target.setSeconds(new Date(execDate.getTime()).getSeconds() + run.slaInfo.errorFinishedBy);
            let missedBy = 0;
            let timeRemaining = 0;
            let missedSLA = false;
            if (endDate > target) {
                missedBy = (endDate.getTime() - target.getTime()) / 1000;
                missedSLA = true;
            } else {
                timeRemaining = (target.getTime() - endDate.getTime()) / 1000;
            }
            slaMissData.push({
                slaType: SLATypes.errorFinishedBy,
                sla: run.slaInfo.errorFinishedBy,
                missedBy,
                timeRemaining,
                missedSLA,
                state,
            } as SLAMissData);
        }

        if (run.slaInfo?.warnStartedBy) {
            const target = new Date(execDate.getTime());
            target.setSeconds(new Date(execDate.getTime()).getSeconds() + run.slaInfo.warnStartedBy);
            let missedBy = 0;
            let timeRemaining = 0;
            let missedSLA = false;
            if (startDate > target) {
                missedBy = (startDate.getTime() - target.getTime()) / 1000;
                missedSLA = true;
            } else {
                timeRemaining = (target.getTime() - startDate.getTime()) / 1000;
            }
            slaMissData.push({
                slaType: SLATypes.warnStartedBy,
                sla: run.slaInfo.warnStartedBy,
                missedBy,
                timeRemaining,
                missedSLA,
                state,
            } as SLAMissData);
        }

        if (run.slaInfo?.warnFinishedBy) {
            const target = new Date(execDate.getTime());
            target.setSeconds(new Date(execDate.getTime()).getSeconds() + run.slaInfo.warnFinishedBy);
            let missedBy = 0;
            let timeRemaining = 0;
            let missedSLA = false;
            if (endDate > target) {
                missedBy = (endDate.getTime() - target.getTime()) / 1000;
                missedSLA = true;
            } else {
                timeRemaining = (target.getTime() - endDate.getTime()) / 1000;
            }
            slaMissData.push({
                slaType: SLATypes.warnFinishedBy,
                sla: run.slaInfo.warnFinishedBy,
                missedBy,
                timeRemaining,
                missedSLA,
                state,
            } as SLAMissData);
        }
    } else {
        // no SLAs were missed nor defined
        slaMissData.push({
            state,
            slaType: SLATypes.noSlaDefined,
            missedSLA: false,
        } as SLAMissData);
    }
    return slaMissData;
}

// get start and landing time values for each run
export function getRunPlotValues(run: DataRunEntity): number[] {
    const execDate = new Date(run.execution.logicalDate).getTime();
    const startDate = new Date(run.execution.startDate).getTime();
    // if not yet landed, use current time
    let endDate = new Date().getTime();
    if (run.execution?.endDate) {
        endDate = new Date(run.execution.endDate).getTime();
    }

    return [(startDate - execDate) / 1000, (endDate - execDate) / 1000];
}

// get color of run column based on state and whether it missed error/warn level SLA
export function getRunColor(run: DataRunEntity): string | undefined {
    if (run?.slaMissData && run.slaMissData.length > 0) {
        // if error level SLA miss, return red
        if (run.slaMissData.some((sla) => sla.missedSLA && isErrorLevelSLA(sla))) {
            return red.primary;
        }
        // if warn level SLA miss, return gold
        if (run.slaMissData.some((sla) => sla.missedSLA && isWarnLevelSLA(sla))) {
            return gold.primary;
        }
    }
    // if run still in progress, return grey
    if (getRunState(run) === 'RUNNING') {
        return grey.primary;
    }
    // default return blue
    return blue.primary;
}

// get SLA description text: how long over/until each defined SLA type
export function getSLAText(
    run: DataRunEntity,
    typeFunction: typeof isStartedSLAType | typeof isFinishedSLAType,
): ReactElement[] {
    const startStepText: ReactElement[] = [];
    if (run?.slaMissData && run.slaMissData.length > 0) {
        run.slaMissData.forEach((sla) => {
            if (typeFunction(sla)) {
                if (sla.missedBy && sla.missedSLA) {
                    startStepText.push(
                        <div>
                            Time delayed over {sla.slaType}: {convertSecsToHumanReadable(sla.missedBy)}
                            <br />
                        </div>,
                    );
                }
                if (!sla.missedSLA && sla.timeRemaining) {
                    startStepText.push(
                        <div>
                            Time remaining until {sla.slaType}: {convertSecsToHumanReadable(sla.timeRemaining)}
                            <br />
                        </div>,
                    );
                }
            }
        });
    }
    return startStepText;
}

// get tool tip on timeliness plot for info about each run including start/end date, SLA misses, try number, airflow link...etc
export function getCustomContentToolTip(items): string {
    const run = items[0]?.data as DataRunEntity;
    if (!run) return `<div></div>`;
    const tryNumber = run?.name ? `<div>Try Number: ${run.name.charAt(run.name.length - 1)}</div>` : '';
    const executionDateString = `<div> Execution Date: ${formatUTCDateString(run.execution.logicalDate)}</div>`;
    const startDateString = `<div> Started At: ${formatUTCDateString(run.execution.startDate, true)}</div>`;
    const endDateString = run.execution?.endDate
        ? `<div>Ended At: ${formatUTCDateString(run.execution.endDate, true)}</div>`
        : '';
    const stateString = `<div>State: ${getRunState(run)}</div>`;
    const runDurationString = run.execution?.endDate
        ? `<div>Task run duration: ${convertSecsToHumanReadable(
              (new Date(run.execution.endDate).getTime() - new Date(run.execution.startDate).getTime()) / 1000,
          )}</div>`
        : `<div>Task run duration: ${convertSecsToHumanReadable(
              (new Date().getTime() - new Date(run.execution.startDate).getTime()) / 1000,
          )}</div>`;
    const slaMissElements = getSLAText(run, isStartedSLAType).concat(getSLAText(run, isFinishedSLAType));
    const slaMissString = slaMissElements.reduce(
        (buildingString, element) => buildingString + renderToString(element),
        '',
    );
    const linkText = '*** Click to view Airflow task logs ***';

    return `<div>${executionDateString}${startDateString}${endDateString}${stateString}${runDurationString}${tryNumber}${slaMissString}${linkText}</div>`;
}

// sort runs in order of execution date and add sla miss data
export function formatRuns(runs: DataRunEntity[]): DataRunEntity[] {
    runs.sort((a, b) => (a.execution.logicalDate > b.execution.logicalDate ? 1 : -1));
    runs.forEach((run) => {
        const currRun = run;
        currRun.slaMissData = getSLAMissData(currRun);
    });
    return runs;
}
