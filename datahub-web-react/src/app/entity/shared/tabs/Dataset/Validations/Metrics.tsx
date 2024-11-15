import React from 'react';
import { DatasetFieldMetrics } from './DatasetFieldMetrics';
import { ANTD_GRAY } from '../../../constants';
import styled from 'styled-components';
import './MetricsTab.less';
import { DatasetMetrics } from './DatasetMetrics';
import { DataQuality, ScoreType, DimensionNameEntity } from '../../../../../../types.generated';
//import { toTitleCaseChangeCase } from '../../../../../dataforge/shared';
import { toLocalDateString } from '../../../../../shared/time/timeUtils';
import { countFormatter } from '../../../../../../utils/formatter';
import { Divider } from 'antd';
import { useListDimensionNamesQuery } from '../../../../../../graphql/dimensionname.generated';

const MetricsContainer = styled.div`
    width: 100%;
    padding-left: 20px;
    padding-top: 20px;
    padding-bottom: 20px;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

export type MetricsProps = {
    metrics: DataQuality;
};

export type QualityMetrics = {
    metric: string;
    current: number;
    historical: number;
    scoreType: string;
    addlInfo?: string;
    note?: string;
};

export type DatasetMetricsProps = {
    metrics: QualityMetrics[];
    title?: string;
};

export const NO_DIMENSION = 'NoDimension';
export const NO_SCORE_DASH = ' - ';
export function getDimensionScoreType(metricDimensions, dimensionNameEntity): [number, number, string, string, string] {
    const result = metricDimensions?.filter((obj) => obj.dimensionUrn === dimensionNameEntity?.urn);
    if (result && result?.length > 0) {
        return [
            result[0].currentScore || '',
            result[0].historicalWeightedScore || '',
            result[0].scoreType || '',
            result[0].note || '',
            '',
        ];
    }
    // default to no score since we must render all dimensions regardless of whether they are present in the data or not.
    // we will differentiate a no score verus a score of 0 by adding a special string called NoDimension.
    return [0, 0, NO_SCORE_DASH, NO_SCORE_DASH, NO_DIMENSION];
}

export function getFormattedScore(scoreType, score): string {
    if (score?.length === 0) return NO_SCORE_DASH;
    if (scoreType === ScoreType.Percentage) {
        return score + '%';
    } else if (scoreType === ScoreType.NumericalValue) {
        return countFormatter(score);
    }
    return NO_SCORE_DASH;
}

export const Metrics = ({ metrics }: MetricsProps) => {
    const metricDimensions = metrics?.datasetDimensionInfo?.dimensions || [];
    

    const getTitle = (): string => {
        let formattedTitle = '';
        const lastModified = metrics?.changeAuditStamps?.lastModified?.time;
        if (metrics?.datasetDimensionInfo?.toolName && metrics?.datasetDimensionInfo?.toolName?.length > 0) {
            formattedTitle = `Updated ${metrics?.datasetDimensionInfo?.toolName}:
                ${
                    lastModified ? toLocalDateString(lastModified) : ''
                } based on ${metrics?.datasetDimensionInfo?.recordCount?.toLocaleString()} record count.`;
        } else {
            formattedTitle = `Updated: ${lastModified ? toLocalDateString(lastModified) : ''}`;
        }
        return formattedTitle;
    };

    let data: QualityMetrics[] = [];
    if (metricDimensions !== null && metricDimensions?.length > 0) {
         // dimension; read custom dimension from urn.
        const { data: listDimensionNameEntity } = useListDimensionNamesQuery({
            variables: {
                input: {
                    query: "*",
                    start: 0,
                    count: 100,
                },
            }
        });
        const customDimensionNames: DimensionNameEntity[] = (listDimensionNameEntity?.listDimensionNames?.dimensionNames) as DimensionNameEntity[]
        // read the dimensionTypeName as it is found instead of filter.
        data = customDimensionNames?.map((dimensionNameEntity) => {
            let [currentScore, historicalWeightedScore, scoreType, note, addlInfo] = getDimensionScoreType(
                metricDimensions,
                dimensionNameEntity
            );
            return {
                metric: (dimensionNameEntity?.info?.name) || '',
                current: currentScore,
                historical: historicalWeightedScore,
                scoreType: scoreType || '',
                note: note || '',
                addlInfo: addlInfo || '',
            };
        }).sort(( a, b ) => a.metric > b.metric ? 1 : -1 );
    }

    return (
        <MetricsContainer>
            <DatasetMetrics metrics={data} title={getTitle()} />
            <Divider className="metrics-divider" />
            <DatasetFieldMetrics metrics={metrics} />
        </MetricsContainer>
    );
};
