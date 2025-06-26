import {
    ArrowDownOutlined,
    ArrowUpOutlined,
    CaretDownFilled,
    CaretDownOutlined,
    LoadingOutlined,
    ReloadOutlined,
    SubnodeOutlined,
} from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Select, Typography } from 'antd';
import * as QueryString from 'query-string';
import React, { useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import ColumnsLineageSelect from '@app/entityV2/shared/tabs/Lineage/ColumnLineageSelect';
import { ImpactAnalysis } from '@app/entityV2/shared/tabs/Lineage/ImpactAnalysis';
import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import LineageTabTimeSelector from '@app/entityV2/shared/tabs/Lineage/LineageTabTimeSelector';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import ManageLineageMenuForImpactAnalysis from '@src/app/entityV2/shared/tabs/Lineage/ManageLineageMenuFromImpactAnalysis';
import { Direction } from '@src/app/lineage/types';

import { EntityType, LineageDirection, LineageSearchPath } from '@types';

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

interface SchemaFieldEntityData extends GenericEntityProperties {
    fieldPath?: string;
}

interface Props {
    defaultDirection: LineageDirection;
    setVisualizeViewInEditMode: (view: boolean, direction: Direction) => void;
}

export function LineageColumnView({ defaultDirection, setVisualizeViewInEditMode }: Props) {
    const { urn, entityType, entityData } = useEntityData();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(defaultDirection);
    const [selectedColumn, setSelectedColumn] = useState<string | undefined>(params?.column as string);
    const [shouldRefetch, setShouldRefetch] = useState(false);
    const [skipCache, setSkipCache] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [lineageSearchPath, setLineageSearchPath] = useState<LineageSearchPath | null>(null);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const entityName = (entityData && entityRegistry.getDisplayName(entityType, entityData)) || '-';

    function resetShouldRefetch() {
        setShouldRefetch(false);
    }

    const columnForLineage = useMemo(
        () =>
            entityType === EntityType.SchemaField ? (entityData as SchemaFieldEntityData)?.fieldPath : selectedColumn,
        [entityType, selectedColumn, entityData],
    );
    const [isColumnLevelLineage, setIsColumnLevelLineage] = useState(!!columnForLineage);

    const selectedV1FieldPath = downgradeV2FieldPath(selectedColumn);
    const onIndividualSiblingPage = useIsSeparateSiblingsMode();
    const lineageUrn = (!onIndividualSiblingPage && entityData?.lineageUrn) || urn;
    const selectedColumnUrn = generateSchemaFieldUrn(selectedV1FieldPath, lineageUrn);
    const impactAnalysisUrn = isColumnLevelLineage && selectedColumnUrn ? selectedColumnUrn : lineageUrn;
    const canEditLineage = !!entityData?.privileges?.canEditLineage;

    const directionOptions = [
        {
            label: (
                <Tooltip
                    placement="right"
                    title={`View the data assets that depend on ${entityName}`}
                    showArrow={false}
                >
                    <span data-testid="lineage-tab-direction-select-option-downstream">
                        <ArrowDownOutlined style={{ marginRight: 4 }} />
                        <b>Downstreams</b>
                    </span>
                </Tooltip>
            ),
            value: LineageDirection.Downstream,
        },
        {
            label: (
                <Tooltip
                    placement="right"
                    title={`View the data assets that ${entityName} depends on`}
                    showArrow={false}
                >
                    <span data-testid="lineage-tab-direction-select-option-upstream">
                        <ArrowUpOutlined style={{ marginRight: 4 }} />
                        <b>Upstreams</b>
                    </span>
                </Tooltip>
            ),
            value: LineageDirection.Upstream,
        },
    ];

    return (
        <>
            <StyledTabToolbar>
                <LeftButtonsWrapper>
                    <StyledSelect
                        bordered={false}
                        value={lineageDirection}
                        options={directionOptions}
                        onChange={(value) => setLineageDirection(value as LineageDirection)}
                        suffixIcon={<CaretDownOutlined style={{ color: 'black' }} />}
                        data-testid="lineage-tab-direction-select"
                    />
                </LeftButtonsWrapper>
                <RightButtonsWrapper>
                    <ManageLineageMenuForImpactAnalysis
                        setVisualizeViewInEditMode={setVisualizeViewInEditMode}
                        menuIcon={
                            <Button type="text">
                                <ManageLineageIcon />
                                <Typography.Text>
                                    <b>Edit</b>
                                </Typography.Text>
                                <StyledCaretDown />
                            </Button>
                        }
                        entityType={entityType}
                        canEditLineage={canEditLineage}
                        disableDropdown={!canEditLineage}
                    />
                    {entityType !== EntityType.SchemaField && (
                        <ColumnsLineageSelect
                            selectedColumn={columnForLineage}
                            isColumnLevelLineage={isColumnLevelLineage}
                            setSelectedColumn={setSelectedColumn}
                            setIsColumnLevelLineage={setIsColumnLevelLineage}
                        />
                    )}
                    <LineageTabTimeSelector />
                    <Tooltip title={isLoading ? 'Refreshing data' : 'Refresh lineage'} showArrow={false}>
                        <RefreshCacheButton type="text" onClick={() => setSkipCache(true)} disabled={isLoading}>
                            {isLoading ? <LoadingOutlined /> : <ReloadOutlined />}
                            <Typography.Text>
                                <b>Refresh</b>
                            </Typography.Text>
                        </RefreshCacheButton>
                    </Tooltip>
                </RightButtonsWrapper>
            </StyledTabToolbar>
            <LineageTabContext.Provider
                value={{
                    isColumnLevelLineage,
                    selectedColumn,
                    lineageDirection,
                    lineageSearchPath,
                    setLineageSearchPath,
                }}
            >
                <ImpactAnalysis
                    urn={impactAnalysisUrn}
                    direction={lineageDirection as LineageDirection}
                    startTimeMillis={startTimeMillis}
                    endTimeMillis={endTimeMillis}
                    skipCache={skipCache}
                    setSkipCache={setSkipCache}
                    shouldRefetch={shouldRefetch}
                    resetShouldRefetch={resetShouldRefetch}
                    setIsLoading={setIsLoading}
                />
            </LineageTabContext.Provider>
        </>
    );
}
