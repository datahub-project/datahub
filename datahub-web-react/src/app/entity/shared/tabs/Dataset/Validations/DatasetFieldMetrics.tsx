import React, { useEffect, useState } from 'react';
import { Card, Form, Input, Table } from 'antd';
import './MetricsTab.less';
import Column from 'antd/lib/table/Column';
import ColumnGroup from 'antd/lib/table/ColumnGroup';
import { MetricsProps, getFormattedScore } from './Metrics';
import styled from 'styled-components';
import { SearchOutlined } from '@ant-design/icons';
import { toLocalDateString } from '../../../../../shared/time/timeUtils';
//import { toTitleCaseChangeCase } from '../../../../../dataforge/shared';
import Icon from '../../../../../../images/Icon.png';
import { useListDimensionNamesQuery } from '../../../../../../graphql/dimensionname.generated';
import { DimensionNameEntity} from '../../../../../../types.generated';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 220px;
    max-height: 32px;
    gap: 2px;
    margin-bottom: 10px;
    border: 1px solid #d9d9d9;
`;

type FieldSearchProps = {
    searchText?: string;
    onFilter?: (value) => any;
};

export const SearchFieldMetrics = ({ searchText, onFilter }: FieldSearchProps): JSX.Element => {
    return (
        <Form.Item label="">
            <StyledInput
                defaultValue={searchText}
                placeholder="Search in Fields..."
                onChange={(val) => {
                    onFilter?.(val.target.value);
                }}
                allowClear
                prefix={<SearchOutlined />}
            />
        </Form.Item>
    );
};

export const FieldMetricsTable = ({ fieldMetrics }: any) => {
    const FieldColumnGroups = () => {
        let metricColumns: any[] = [];
        // get all dimension names from enum. display all dimension names as columns regardless of whether they are present in the data or not
        //const dimensions = Object.values(DimensionName).sort();
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
        customDimensionNames?.forEach((dimensionNameEntity) => {
            const dimensionTitle = (dimensionNameEntity?.info?.name.replace(/_/g, ' '));
            const dimensionVal: string = dimensionNameEntity?.info?.name.toLowerCase() as string;
            metricColumns.push(
                <ColumnGroup title={dimensionTitle} dataIndex={dimensionVal} key={dimensionVal}>
                    <Column
                        title="hist."
                        dataIndex={dimensionVal.concat('_historicalScore')}
                        key={dimensionVal.concat('_historicalScore')}
                    />
                    <Column
                        title="cur."
                        dataIndex={dimensionVal.concat('_currentScore')}
                        key={dimensionVal.concat('_currentScore')}
                    />
                </ColumnGroup>,
            );
        });
        return (
            <Table bordered dataSource={fieldMetrics} className="field-metrics-table" pagination={false} scroll={{ x: 300 }}>
                <ColumnGroup title="Field Name" dataIndex="fieldName" key="fieldName">
                    <Column title="" dataIndex="fieldName" key="fieldName" />
                </ColumnGroup>
                <ColumnGroup title="Record Count" dataIndex="recordCount" key="recordCount">
                    <Column title="" dataIndex="recordCount" key="recordCount" />
                </ColumnGroup>
                {metricColumns}
            </Table>
        );
    };

    return (
        <>
            <FieldColumnGroups />
            <p className="section-note">
                *This table is based on data provided by the producer which can be either percentages or counts.
            </p>
        </>
    );
};

const getFieldNameFromUrn = (urn: string) => {
    if (urn && urn?.length > 0) {
        return urn?.substring(urn.lastIndexOf(',') + 1, urn.length - 1) || '';
    }
    return '';
};

export const DatasetFieldMetrics = ({ metrics }: MetricsProps) => {
    const [searchText, setSearchText] = useState('');
    const [allQualityMetrics, setAllQualityMetrics] = useState<any>(null);
    const [filteredQualityMetrics, setFilteredQualityMetrics] = useState<any>(null);
    const [lastModified, setLastModified] = useState<any>(null);

    useEffect(() => {
        setAllQualityMetrics(metrics?.schemaFieldDimensionInfos);
        setFilteredQualityMetrics(metrics?.schemaFieldDimensionInfos);
        setLastModified(metrics?.changeAuditStamps?.lastModified.time);
    }, [metrics, metrics?.schemaFieldDimensionInfos]);

    const searchFieldMetrics = (value) => {
        if (!allQualityMetrics) {
            return;
        }
        if (value && value?.length > 0) {
            setSearchText(value);
            const metrics = [...allQualityMetrics];
            setFilteredQualityMetrics(
                metrics?.filter((metric) => JSON.stringify(metric)?.toLowerCase().includes(value.toLowerCase())),
            );
        } else {
            setSearchText('');
            const metrics = [...allQualityMetrics];
            setFilteredQualityMetrics(metrics);
        }
    };

    function flattenDimensions(obj) {
        return Object.keys(obj).reduce((dimension, index) => {
            if (typeof obj[index] === 'object' && obj[index] !== null) {
                Object.assign(dimension, flattenDimensions(obj[index]));
            } else {
                dimension[index] = obj[index];
            }
            return dimension;
        }, {});
    }

    function getUniqueToolNames(metrics) {
        const toolNames = metrics?.map((metric) => metric?.schemaFieldDimensionInfo?.toolName);
        if (toolNames && toolNames?.length > 0) {
            const uniqueToolNames = toolNames?.filter((value, index, toolNames) => toolNames.indexOf(value) === index);
            return uniqueToolNames?.length > 1 ? uniqueToolNames?.join(', ') : uniqueToolNames;
        }
        return '';
    }

    const { data: listDimensionNameEntity } = useListDimensionNamesQuery({
        variables: {
            input: {
                query: "*",
                start: 0,
                count: 100,
            },
        }
    });
    const customDimensionNamesEntitys: DimensionNameEntity[] = (listDimensionNameEntity?.listDimensionNames?.dimensionNames) as DimensionNameEntity[]
    let fieldMetrics: any[] = [];
    if (filteredQualityMetrics !== null) {
        filteredQualityMetrics?.forEach((d) => {
            const fieldDimension = {
                key: d?.schemaFieldUrn.toString() || '',
                fieldName: getFieldNameFromUrn(d?.schemaFieldUrn || ''),
                recordCount: d?.schemaFieldDimensionInfo?.recordCount?.toLocaleString() || '',
                obj: d?.schemaFieldDimensionInfo?.dimensions?.map((dimension) => {
                    const customDimensionNameEntity = Array.isArray(customDimensionNamesEntitys)
                            ? customDimensionNamesEntitys.filter(customDimensionNameEntity => customDimensionNameEntity?.urn === dimension.dimensionUrn)[0]
                            : undefined;

                    if (!customDimensionNameEntity) {
                        console.error('customDimensionNameEntity is undefined or not found');
                    }
                    const dimensionName = customDimensionNameEntity?.info?.name || '';
                    const currentKey = dimensionName.toLowerCase().concat('_currentScore');
                    const historicalKey = dimensionName.toLowerCase().concat('_historicalScore');
                    return {
                        dimension_name: customDimensionNameEntity?.info?.name,
                        [currentKey]: getFormattedScore(dimension.scoreType, dimension.currentScore),
                        [historicalKey]: getFormattedScore(dimension.scoreType, dimension.historicalWeightedScore),
                    };
                }),
            };
            const flat = flattenDimensions(fieldDimension.obj);
            fieldMetrics.push({
                key: fieldDimension.key,
                fieldName: fieldDimension.fieldName,
                recordCount: fieldDimension.recordCount,
                ...flat,
            });
        });

        return (
            <div className="MetricsTab">
                <p className="section-heading">Field Metrics</p>
                <p className="section-sub-heading">
                    Updated {getUniqueToolNames(filteredQualityMetrics)}: {toLocalDateString(lastModified)}
                </p>
                <Form layout="vertical" className="filter-section">
                    <SearchFieldMetrics searchText={searchText} onFilter={searchFieldMetrics} />
                </Form>
                {fieldMetrics && <FieldMetricsTable fieldMetrics={fieldMetrics} />}
            </div>
        );
    } else {
        return (
            <Card className="error-card">
                <img className="card-error-img" src={Icon} alt="filter" />
                <span className="error-text">There are no schema field metrics available for this dataset.</span>
            </Card>
        );
    }
};
