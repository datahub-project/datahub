import React, { FC } from 'react';
import { Descriptions, Tag } from 'antd';
import { blue, red, gold } from '@ant-design/colors';
import { DataRunEntity, SLATypes } from '../interfaces';
import { LatestRunSteps } from './LatestRunSteps';
import { SlaInfo } from '../../../../../../../types.generated';
import { convertSecsToHumanReadable } from '../functions';

// get the color of the tag depending on how high the miss type, blue if not missed
function getTagColor(missVal: number, missType: SLATypes): string | undefined {
    if (!missVal || missVal === 0) return blue.primary;
    if (missType === SLATypes.warnStartedBy || missType === SLATypes.warnFinishedBy) return gold.primary;
    return red.primary;
}

// get the SLA miss percentage for the give SLA miss type
const SLAMissPercentTag = ({ missType, runs }: { missType: SLATypes; runs: DataRunEntity[] }) => {
    // get number of SLA misses for the given miss type
    let missedSLACount = 0;
    runs.forEach((run) => {
        if (run?.slaMissData && run.slaMissData.length > 0) {
            run.slaMissData.forEach((slaMiss) => {
                if (slaMiss.missedSLA && slaMiss.slaType === missType) {
                    missedSLACount += 1;
                }
            });
        }
    });
    // SLA miss rate is number of misses / total number of runs
    const rate = Math.round((missedSLACount / runs.length) * 10000.0) / 100;
    return <Tag color={getTagColor(rate, missType)}>{rate.toString()}%</Tag>;
};

// get SLA miss delay average for the given SLA miss type
const SLAMissDelayAverageTag = ({ missType, runs }: { missType: SLATypes; runs: DataRunEntity[] }) => {
    let slaMissSeconds = 0;
    let slaMissCount = 0;
    runs.forEach((run) => {
        if (run?.slaMissData && run.slaMissData.length > 0) {
            run.slaMissData.forEach((slaMiss) => {
                if (slaMiss.missedSLA && slaMiss.missedBy && slaMiss.slaType === missType) {
                    slaMissSeconds += slaMiss.missedBy;
                    slaMissCount += 1;
                }
            });
        }
    });
    // delay average is total number of seconds runs have missed SLA by / number of runs that missed SLA
    const delayAvg = slaMissSeconds / slaMissCount;
    return (
        <Tag color={getTagColor(delayAvg, missType)}>{!delayAvg ? '0Min' : convertSecsToHumanReadable(delayAvg)}</Tag>
    );
};

const renderSLAMissInfoRow = ({
    slaType,
    missTypeString,
    latestSLAInfo,
    runs,
}: {
    slaType: string;
    missTypeString: string;
    latestSLAInfo?: SlaInfo;
    runs: DataRunEntity[];
}) => {
    // if any of the runs have this SLA type, calculate miss percentage and average delay across all runs
    if (runs.some((r) => r?.slaInfo?.[missTypeString])) {
        return (
            <>
                <Descriptions.Item label={`Latest Run ${slaType}`}>
                    {latestSLAInfo?.[missTypeString] ? convertSecsToHumanReadable(latestSLAInfo[missTypeString]) : '-'}
                </Descriptions.Item>
                <Descriptions.Item label={`% Runs Missed ${slaType}`}>
                    <SLAMissPercentTag missType={SLATypes[missTypeString]} runs={runs} />
                </Descriptions.Item>
                <Descriptions.Item label={`${slaType} Delay Average`}>
                    <SLAMissDelayAverageTag missType={SLATypes[missTypeString]} runs={runs} />
                </Descriptions.Item>
            </>
        );
    }
    return null;
};

/**
 * Data Table for SLA miss percentages and delay averages
 * @param runs
 * @param latestSLAInfo current SLA info set on Data entity (this will be equal to SLA on its latest run)
 */
export const SLADataTable: FC<{
    runs: DataRunEntity[];
    latestSLAInfo?: SlaInfo;
}> = ({ runs, latestSLAInfo }) => {
    return (
        <Descriptions title="" bordered>
            <Descriptions.Item label="Latest run" span={3}>
                <LatestRunSteps run={runs[runs.length - 1]} />
            </Descriptions.Item>
            {[
                { slaType: 'Warn Start SLA', missTypeString: 'warnStartedBy' },
                { slaType: 'Warn End SLA', missTypeString: 'warnFinishedBy' },
                { slaType: 'Error Start SLA', missTypeString: 'errorStartedBy' },
                { slaType: 'Error End SLA', missTypeString: 'errorFinishedBy' },
            ].map((slaData) => renderSLAMissInfoRow({ runs, latestSLAInfo, ...slaData }))}
        </Descriptions>
    );
};
