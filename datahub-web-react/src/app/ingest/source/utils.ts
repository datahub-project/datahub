import { colors } from '@components';
import {
    ArrowCounterClockwise,
    ArrowsCounterClockwise,
    Checks,
    ClockClockwise,
    Prohibit,
    Spinner,
    X,
} from 'phosphor-react';
import YAML from 'yamljs';
import { SourceConfig } from '@app/ingest/source/builder/types';
import { ExecutionRequestResult } from '@types';

export const getSourceConfigs = (ingestionSources: SourceConfig[], sourceType: string) => {
    const sourceConfigs = ingestionSources.find((source) => source.name === sourceType);
    if (!sourceConfigs) {
        console.error(`Failed to find source configs with source type ${sourceType}`);
    }
    return sourceConfigs;
};

export const yamlToJson = (yaml: string): string => {
    const obj = YAML.parse(yaml);
    const jsonStr = JSON.stringify(obj);
    return jsonStr;
};

export const jsonToYaml = (json: string): string => {
    const obj = JSON.parse(json);
    const yamlStr = YAML.stringify(obj, 6);
    return yamlStr;
};

export function getPlaceholderRecipe(ingestionSources: SourceConfig[], type?: string) {
    const selectedSource = ingestionSources.find((source) => source.name === type);
    return selectedSource?.recipe || '';
}

export const RUNNING = 'RUNNING';
export const SUCCESS = 'SUCCESS';
const SUCCEEDED_WITH_WARNINGS = 'SUCCEEDED_WITH_WARNINGS';
export const FAILURE = 'FAILURE';
const CANCELLED = 'CANCELLED';
const ABORTED = 'ABORTED';
const UP_FOR_RETRY = 'UP_FOR_RETRY';
export const ROLLING_BACK = 'ROLLING_BACK';
export const ROLLED_BACK = 'ROLLED_BACK';
const ROLLBACK_FAILED = 'ROLLBACK_FAILED';

export const CLI_EXECUTOR_ID = '__datahub_cli_';

export const getExecutionRequestStatusIcon = (status?: string) => {
    return (
        (status === RUNNING && Spinner) ||
        (status === SUCCESS && Checks) ||
        (status === SUCCEEDED_WITH_WARNINGS && Checks) ||
        (status === FAILURE && X) ||
        (status === CANCELLED && Prohibit) ||
        (status === UP_FOR_RETRY && ClockClockwise) ||
        (status === ROLLED_BACK && ArrowCounterClockwise) ||
        (status === ROLLING_BACK && ArrowsCounterClockwise) ||
        (status === ROLLBACK_FAILED && X) ||
        (status === ABORTED && X) ||
        ClockClockwise
    );
};

export const getExecutionRequestStatusDisplayText = (status?: string) => {
    return (
        (status === RUNNING && 'Running') ||
        (status === SUCCESS && 'Succeeded') ||
        (status === SUCCEEDED_WITH_WARNINGS && 'Succeeded With Warnings') ||
        (status === FAILURE && 'Failed') ||
        (status === CANCELLED && 'Cancelled') ||
        (status === UP_FOR_RETRY && 'Up for Retry') ||
        (status === ROLLED_BACK && 'Rolled Back') ||
        (status === ROLLING_BACK && 'Rolling Back') ||
        (status === ROLLBACK_FAILED && 'Rollback Failed') ||
        (status === ABORTED && 'Aborted') ||
        status
    );
};

export const getExecutionRequestStatusDisplayColor = (status?: string) => {
    return (
        (status === RUNNING && colors.blue[1000]) ||
        (status === SUCCESS && colors.green[500]) ||
        (status === SUCCEEDED_WITH_WARNINGS && colors.yellow[500]) ||
        (status === FAILURE && colors.red[500]) ||
        (status === UP_FOR_RETRY && colors.violet[600]) ||
        (status === CANCELLED && colors.gray[1700]) ||
        (status === ROLLED_BACK && colors.yellow[500]) ||
        (status === ROLLING_BACK && colors.yellow[500]) ||
        (status === ROLLBACK_FAILED && colors.red[500]) ||
        (status === ABORTED && colors.red[500]) ||
        colors.gray[1700]
    );
};

export const validateURL = (fieldName: string) => {
    return {
        validator(_, value) {
            const URLPattern = new RegExp(
                /^(?:http(s)?:\/\/)?[\w.-]+(?:\.[a-zA-Z0-9.-]{2,})+[\w\-._~:/?#[\]@!$&'()*+,;=.]+$/,
            );
            const isURLValid = URLPattern.test(value);
            if (!value || isURLValid) {
                return Promise.resolve();
            }
            return Promise.reject(new Error(`A valid ${fieldName} is required.`));
        },
    };
};

const extractStructuredReportPOJO = (result: Partial<ExecutionRequestResult>): any | null => {
    const structuredReportStr = result?.structuredReport?.serializedValue;
    if (!structuredReportStr) {
        return null;
    }
    try {
        return JSON.parse(structuredReportStr);
    } catch (e) {
        console.error(`Caught exception while parsing structured report!`, e);
        return null;
    }
};

/** *
 * This function is used to get the entities ingested by type from the structured report.
 * It returns an array of objects with the entity type and the count of entities ingested.
 *
 * Example input:
 *
 * {
 *     "source": {
 *         "report": {
 *             "aspects": {
 *                 "container": {
 *                     "containerProperties": 156,
 *                     ...
 *                     "container": 117
 *                 },
 *                 "dataset": {
 *                     "datasetProperties": 1505,
 *                     ...
 *                     "operation": 1521
 *                 },
 *                 ...
 *             }
 *             ...
 *         }
 *     }
 *     ...
 * }
 *
 * Example output:
 *
 * [
 *     {
 *         "count": 156,
 *         "displayName": "container"
 *     },
 *     ...
 * ]
 *
 * @param result - The result of the execution request.
 * @returns {EntityTypeCount[] | null}
 */
export const getEntitiesIngestedByType = (result: Partial<ExecutionRequestResult>): EntityTypeCount[] | null => {
    const structuredReportObject = extractStructuredReportPOJO(result);
    if (!structuredReportObject) {
        return null;
    }

    try {
        /**
         * This is what the aspects object looks like in the structured report:
         *
         * "aspects": {
         *     "container": {
         *         "containerProperties": 156,
         *         ...
         *         "container": 117
         *     },
         *     "dataset": {
         *         "status": 1505,
         *         "schemaMetadata": 1505,
         *         "datasetProperties": 1505,
         *         "container": 1505,
         *         ...
         *         "operation": 1521
         *     },
         *     ...
         * }
         */
        const entities = structuredReportObject.source.report.aspects;
        const entitiesIngestedByType: { [key: string]: number } = {};
        Object.entries(entities).forEach(([entityName, aspects]) => {
            // Get the max count of all the sub-aspects for this entity type.
            entitiesIngestedByType[entityName] = Math.max(...(Object.values(aspects as object) as number[]));
        });

        if (Object.keys(entitiesIngestedByType).length === 0) {
            return null;
        }

        return Object.entries(entitiesIngestedByType).map(([entityName, count]) => ({
            count,
            displayName: entityName,
        }));
    } catch (e) {
        console.error(`Caught exception while parsing structured report!`, e);
        return null;
    }
};

type EntityTypeCount = {
    count: number;
    displayName: string;
};
