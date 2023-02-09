import { grey, orange, red, green, blue, gold } from '@ant-design/colors';
import { SlaInfo } from '../../../../../../types.generated';

export const stateToStepMapping = {
    SCHEDULED: 1,
    RUNNING: 2,
    SUCCESS: 3,
    FAILURE: 3,
    SKIPPED: 3,
    UP_FOR_RETRY: 3,
};

export const stateToColorMapping = {
    RUNNING: grey.primary,
    SUCCESS: green.primary,
    FAILURE: red.primary,
    SKIPPED: grey[grey.length - 1],
    UP_FOR_RETRY: orange.primary,
};

/* eslint-disable @typescript-eslint/no-non-null-assertion */
// Valid useage of non-null coercion
export const plotColorLegendMapping = {
    [red.primary!]: 'Error Level SLA Miss',
    [gold.primary!]: 'Warn Level SLA Miss',
    [grey.primary!]: 'Running',
    [blue.primary!]: 'Did Not Miss SLA',
};
/* eslint-enable @typescript-eslint/no-non-null-assertion */

export enum SLATypes {
    warnStartedBy = 'warn started SLA',
    warnFinishedBy = 'warn finished SLA',
    errorStartedBy = 'error started SLA',
    errorFinishedBy = 'error finished SLA',
    noSlaDefined = 'no SLA defined',
}

export type SLAMissData = {
    state: string;
    missedSLA: boolean;
    missedBy?: number | null;
    timeRemaining?: number | null;
    sla?: number | null;
    slaType: SLATypes;
};

export type DataRunEntity = {
    name: string;
    execution: {
        logicalDate: number;
        startDate: number;
        endDate?: number | null;
    };
    state:
        | {
              status: string;
              result?: {
                  resultType?: string;
              } | null;
          }[];
    externalUrl: string;
    slaInfo?: SlaInfo | null;
    slaMissData: SLAMissData[] | undefined;
};
