import React, { useCallback, useState } from 'react';
import { Button, Select, Tooltip, Typography } from 'antd';
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import {
    ArrowDownOutlined,
    ArrowUpOutlined,
    CaretDownFilled,
    CaretDownOutlined,
    PartitionOutlined,
    ReloadOutlined,
    SubnodeOutlined,
} from '@ant-design/icons';
import styled from 'styled-components/macro';

import { useEntityData } from '../../EntityContext';
import TabToolbar from '../../components/styled/TabToolbar';
import { getEntityPath } from '../../containers/profile/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ImpactAnalysis } from './ImpactAnalysis';
import { LineageDirection } from '../../../../../types.generated';
import { generateSchemaFieldUrn } from './utils';
import { downgradeV2FieldPath } from '../../../dataset/profile/schema/utils/utils';
import ColumnsLineageSelect from './ColumnLineageSelect';
import { LineageTabContext } from './LineageTabContext';
import ManageLineageMenu from '../../../../lineage/manage/ManageLineageMenu';
import LineageTabTimeSelector from './LineageTabTimeSelector';
import { useGetLineageTimeParams } from '../../../../lineage/utils/useGetLineageTimeParams';
import { ANTD_GRAY } from '../../constants';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: space-between;
    z-index: 2;
`;

const LeftButtonsWrapper = styled.div`
    align-items: center;
    display: flex;
`;

const RightButtonsWrapper = styled.div`
    align-items: center;
    display: flex;
`;

const ManageLineageIcon = styled(SubnodeOutlined)`
    &&& {
        margin-right: -2px;
    }
`;

const StyledCaretDown = styled(CaretDownFilled)`
    &&& {
        font-size: 10px;
        margin-left: 4px;
    }
`;

const StyledSelect = styled(Select)`
    &:hover {
        background-color: ${ANTD_GRAY[2]};
    }
`;

const RefreshCacheButton = styled(Button)`
    margin-left: 8px;
`;

export const LineageTab = ({
    properties = { defaultDirection: LineageDirection.Downstream },
}: {
    properties?: { defaultDirection: LineageDirection };
}) => {
    const { urn, entityType, entityData } = useEntityData();
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(properties.defaultDirection);
    const [selectedColumn, setSelectedColumn] = useState<string | undefined>(params?.column as string);
    const [isColumnLevelLineage, setIsColumnLevelLineage] = useState(!!params?.column);
    const [shouldRefetch, setShouldRefetch] = useState(false);
    const [skipCache, setSkipCache] = useState(false);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    function resetShouldRefetch() {
        setShouldRefetch(false);
    }

    const routeToLineage = useCallback(() => {
        history.push(
            getEntityPath(entityType, urn, entityRegistry, true, false, 'Lineage', {
                start_time_millis: startTimeMillis,
                end_time_millis: endTimeMillis,
            }),
        );
    }, [history, entityType, urn, entityRegistry, startTimeMillis, endTimeMillis]);

    const selectedV1FieldPath = downgradeV2FieldPath(selectedColumn) || '';
    const selectedColumnUrn = generateSchemaFieldUrn(selectedV1FieldPath, urn);
    const impactAnalysisUrn = isColumnLevelLineage && selectedColumnUrn ? selectedColumnUrn : urn;
    const canEditLineage = !!entityData?.privileges?.canEditLineage;

    const directionOptions = [
        {
            label: (
                <span data-testid="lineage-tab-direction-select-option-downstream">
                    <ArrowDownOutlined style={{ marginRight: 4 }} />
                    <b>Downstream</b>
                </span>
            ),
            value: LineageDirection.Downstream,
        },
        {
            label: (
                <span data-testid="lineage-tab-direction-select-option-upstream">
                    <ArrowUpOutlined style={{ marginRight: 4 }} />
                    <b>Upstream</b>
                </span>
            ),
            value: LineageDirection.Upstream,
        },
    ];

    return (
        <>
            <StyledTabToolbar>
                <LeftButtonsWrapper>
                    <ManageLineageMenu
                        entityUrn={urn}
                        refetchEntity={() => setShouldRefetch(true)}
                        setUpdatedLineages={() => {}}
                        menuIcon={
                            <Button type="text">
                                <ManageLineageIcon />
                                <Typography.Text>
                                    <b>Edit</b>
                                </Typography.Text>
                                <StyledCaretDown />
                            </Button>
                        }
                        showLoading
                        entityType={entityType}
                        entityPlatform={entityData?.platform?.name}
                        canEditLineage={canEditLineage}
                        disableDropdown={!canEditLineage}
                    />
                    <Button type="text" onClick={routeToLineage}>
                        <PartitionOutlined />
                        <Typography.Text>
                            <b>Visualize Lineage</b>
                        </Typography.Text>
                    </Button>
                </LeftButtonsWrapper>
                <RightButtonsWrapper>
                    <StyledSelect
                        bordered={false}
                        value={lineageDirection}
                        options={directionOptions}
                        onChange={(value) => setLineageDirection(value as LineageDirection)}
                        suffixIcon={<CaretDownOutlined style={{ color: 'black' }} />}
                        data-testid="lineage-tab-direction-select"
                    />
                    <ColumnsLineageSelect
                        selectedColumn={selectedColumn}
                        isColumnLevelLineage={isColumnLevelLineage}
                        setSelectedColumn={setSelectedColumn}
                        setIsColumnLevelLineage={setIsColumnLevelLineage}
                    />
                    <LineageTabTimeSelector />
                    <Tooltip title="Click to refresh data">
                        <RefreshCacheButton type="text" onClick={() => setSkipCache(true)}>
                            <ReloadOutlined />
                            <Typography.Text>
                                <b>Refresh</b>
                            </Typography.Text>
                        </RefreshCacheButton>
                    </Tooltip>
                </RightButtonsWrapper>
            </StyledTabToolbar>
            <LineageTabContext.Provider value={{ isColumnLevelLineage, selectedColumn, lineageDirection }}>
                <ImpactAnalysis
                    urn={impactAnalysisUrn}
                    onLineageClick={routeToLineage}
                    isLineageTab
                    direction={lineageDirection as LineageDirection}
                    startTimeMillis={startTimeMillis}
                    endTimeMillis={endTimeMillis}
                    skipCache={skipCache}
                    setSkipCache={setSkipCache}
                    shouldRefetch={shouldRefetch}
                    resetShouldRefetch={resetShouldRefetch}
                />
            </LineageTabContext.Provider>
        </>
    );
};
