import React, { useState } from 'react';
import { ScoreType } from '../../../../../../types.generated';
import { DatasetMetricsProps, NO_DIMENSION, QualityMetrics, getFormattedScore } from './Metrics';
import { Group } from '@visx/group';
import { BarGroup } from '@visx/shape';
import { AxisBottom, AxisLeft } from '@visx/axis';
import { scaleBand, scaleLinear, scaleOrdinal } from '@visx/scale';
import './MetricsTab.less';
import { Card, Descriptions } from 'antd';
import Icon from '../../../../../../images/Union.svg';
import { countFormatter } from '../../../../../../utils/formatter';
import { useListDimensionNamesQuery } from '../../../../../../graphql/dimensionname.generated';
import { DimensionNameEntity} from '../../../../../../types.generated';

export type MetricsBarGroupProps = {
    width: number;
    height: number;
    margin?: { top: number; right: number; bottom: number; left: number };
    metrics?: QualityMetrics[];
    addlInfo?: string;
};

type LegendLineProps = {
    legendType: string;
};

const CHART_COLORS = {
    CURRENT: '#FA8C16',
    HISTORY: '#1990ff',
    BACKGROUND: '#FFFFFF',
    STROKE: '#000000',
};

// set some defaults
const DEFAULT_MARGIN = { top: 40, right: 0, bottom: 40, left: 0 };
const CURRENT_SCORE = 'Current';
const HISTORICAL_SCORE = 'Historical';
const STROKE_FONT_SIZE = 11;
const BAR_GROUP_WIDTH = 1102;
const BAR_GROUP_HEIGHT = 260;
const BAR_RECT_RADIUS = 4;
const MAX_YAXIS_TICKS = 3;
const XAXIS_PADDING_LEFT = 15;

export const DatasetMetricsChart = ({ metrics }: DatasetMetricsProps) => {
    const [isMultipleScoreType, setMultipleScoreType] = useState(false);
    const [scoreType, setScoreType] = useState(ScoreType.Percentage);

    // determines if the dimensions contain both score types (percentage and count)
    function multipleScoreToolTypes(metrics) {
        const scoreTypes = metrics
            ?.filter((metric) => metric?.scoreType?.length > 0 && metric?.scoreType !== ' - ')
            .map((metric) => metric?.scoreType);
        if (scoreTypes && scoreTypes?.length > 0) {
            const uniqueScoreTypes = scoreTypes?.filter(
                (value, index, scoreTypes) => scoreTypes.indexOf(value) === index,
            );
            if (uniqueScoreTypes?.length === 1) {
                setScoreType(uniqueScoreTypes[0]);
            }
            return uniqueScoreTypes?.length > 1;
        }
        return true;
    }

    const MetricsBarGroup = ({ width, height, margin = DEFAULT_MARGIN, metrics }: MetricsBarGroupProps) => {
        setMultipleScoreType(multipleScoreToolTypes(metrics));
        const keys = Object.keys(metrics && metrics?.length > 0 ? metrics[0] : []).filter(
            (key) => key === 'current' || key === 'historical',
        ).reverse();

        const getMetric = (d: QualityMetrics) => d.metric.replace(/_/g, ' ');

        // x0 is the metric name to display on the x-axis
        const metricValueScale = scaleBand<string>({
            domain: metrics?.map(getMetric),
            padding: 0.2,
        });

        // x1 is the current and historical scores for each metric to display as a bar
        const metricsScale = scaleBand<string>({
            domain: keys,
            padding: 0.1,
        });

        const getMaxVal = () => {
            const maxVal = Math.max(...(metrics?.map((d) => Math.max(...keys.map((key) => Number(d[key])))) || [0]));
            // if the max value is 0, set it to 100 to display the y-axis, so we do not show 0% bars
            return maxVal === 0 ? 100 : maxVal;
        }
        // score value displayed on the y-axis as a percentage or count
        const yAxisScale = scaleLinear<number>({
            domain: [
                0,
                !isMultipleScoreType
                    ? getMaxVal()
                    : 0,
            ],
            nice: true,
        });

        // color of the bar for current and historical scores
        const colorScale = scaleOrdinal<string, string>({
            domain: keys,
            range: [CHART_COLORS.HISTORY, CHART_COLORS.CURRENT],
        });

        const xMax = width - margin.left - margin.right;
        const yMax = height - margin.top - margin.bottom;
        metricValueScale.rangeRound([0, xMax]);
        metricsScale.rangeRound([0, metricValueScale.bandwidth()]);
        yAxisScale.range([yMax, 0]);

        const getDimensionValue = (dimension, legendType): string => {
            if (dimension?.addlInfo === NO_DIMENSION) return '     - ';
            const score = legendType === CURRENT_SCORE ? dimension.current : dimension.historical;
            return getFormattedScore(dimension.scoreType, score);
        };

        const formatYAxis = (value) => {
            if (scoreType === ScoreType.NumericalValue) {
                return countFormatter(value);
            } else if (scoreType === ScoreType.Percentage) {
                return value + '%';
            }
        };

        // map the dimensions along with the current and historical scores in the legend
        const LegendRow = ({ legendType }: LegendLineProps): JSX.Element => {
            //const dimensions = Object.values(DimensionName);
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
            return (
                <Descriptions column={customDimensionNames?.length + 1} className="legend-table">
                    <Descriptions.Item key={'title'}>
                        <svg className="legend-rect">
                            <rect
                                className="legend-rect"
                                fill={legendType === CURRENT_SCORE ? CHART_COLORS.CURRENT : CHART_COLORS.HISTORY}
                            />
                        </svg>
                        &nbsp;
                        <span className="legend-table-type">{legendType}</span>
                    </Descriptions.Item>
                    {metrics?.map((dimension, index) => (
                        <Descriptions.Item key={index}>
                            <span className="legend-table-dimension">{getDimensionValue(dimension, legendType)}</span>
                        </Descriptions.Item>
                    ))}
                </Descriptions>
            );
        };

        const MetricsLegend = (): JSX.Element => {
            return (
                <div className="legend-section">
                    <LegendRow legendType={HISTORICAL_SCORE} />
                    <LegendRow legendType={CURRENT_SCORE} />
                </div>
            );
        };

        return (
            <>
                {isMultipleScoreType && (
                    <Card className="error-card">
                        <img className="card-error-img" src={Icon} alt="filter" />
                        <span className="error-text">
                            The chart can't be generated because of mixed values format (percentages and counts)
                        </span>
                    </Card>
                )}
                <div className="metrics-container">
                    <div className="chart-section">
                        <svg width={width} height={height}>
                            <rect x={0} y={0} width={width} height={height} fill={CHART_COLORS.BACKGROUND} rx={14} />
                            <Group top={margin.top} left={margin.left}>
                                <BarGroup
                                    data={metrics ? (isMultipleScoreType ? [] : metrics) : []}
                                    keys={keys}
                                    height={yMax}
                                    x0={getMetric}
                                    x0Scale={metricValueScale}
                                    x1Scale={metricsScale}
                                    yScale={yAxisScale}
                                    left={margin.left + 300}
                                    color={colorScale}
                                >
                                    {(barGroups) =>
                                        barGroups.map((barGroup) => (
                                            <Group
                                                key={`bar-group-${barGroup.index}-${barGroup.x0}`}
                                                left={barGroup.x0 + XAXIS_PADDING_LEFT}
                                            >
                                                {barGroup.bars.map((bar) => (
                                                    <rect
                                                        key={`bar-group-bar-${barGroup.index}-${bar.index}-${bar.value}-${bar.key}`}
                                                        x={bar.x}
                                                        y={bar.y + 1}
                                                        width={bar.width}
                                                        height={bar.height}
                                                        fill={bar.color}
                                                        rx={BAR_RECT_RADIUS}
                                                    />
                                                ))}
                                            </Group>
                                        ))
                                    }
                                </BarGroup>

                                <AxisBottom
                                    top={yMax + 1}
                                    scale={metricValueScale}
                                    stroke={CHART_COLORS.STROKE}
                                    tickStroke={CHART_COLORS.STROKE}
                                    hideTicks
                                    tickLabelProps={() => ({
                                        fill: CHART_COLORS.STROKE,
                                        fontSize: STROKE_FONT_SIZE,
                                        textAnchor: 'middle',
                                    })}
                                    left={margin.left + XAXIS_PADDING_LEFT}
                                />
                                <AxisLeft
                                    scale={yAxisScale}
                                    stroke={CHART_COLORS.STROKE}
                                    tickStroke={CHART_COLORS.STROKE}
                                    numTicks={MAX_YAXIS_TICKS}
                                    left={margin.left + XAXIS_PADDING_LEFT}
                                    orientation="left"
                                    hideTicks
                                    hideAxisLine
                                    tickFormat={formatYAxis}
                                    tickLabelProps={() => ({
                                        fill: CHART_COLORS.STROKE,
                                        fontSize: STROKE_FONT_SIZE,
                                        textAnchor: 'start',
                                    })}
                                />
                            </Group>
                        </svg>
                    </div>
                    <MetricsLegend />
                </div>
            </>
        );
    };

    return (
        <div className="MetricsTab">
            <br />
            <MetricsBarGroup width={BAR_GROUP_WIDTH} height={BAR_GROUP_HEIGHT} metrics={metrics} />
            <p className="section-note">*The quality tool used will determine which metrics are shown.</p>
        </div>
    );
};
