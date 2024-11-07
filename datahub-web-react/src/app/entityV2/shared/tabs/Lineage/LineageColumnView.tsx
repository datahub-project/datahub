import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import React, { useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components/macro';
import * as QueryString from 'query-string';
import {
    ArrowDownOutlined,
    ArrowUpOutlined,
    CaretDownFilled,
    CaretDownOutlined,
    ReloadOutlined,
    SubnodeOutlined,
    LoadingOutlined,
} from '@ant-design/icons';
import { Button, Select, Typography } from 'antd';
import { Tooltip } from '@components';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { EntityType, LineageDirection } from '../../../../../types.generated';
import ManageLineageMenu from '../../../../lineage/manage/ManageLineageMenu';
import { useGetLineageTimeParams } from '../../../../lineage/utils/useGetLineageTimeParams';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { downgradeV2FieldPath } from '../../../dataset/profile/schema/utils/utils';
import TabToolbar from '../../components/styled/TabToolbar';
import { ANTD_GRAY } from '../../constants';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import ColumnsLineageSelect from './ColumnLineageSelect';
import { ImpactAnalysis } from './ImpactAnalysis';
import { LineageTabContext } from './LineageTabContext';
import LineageTabTimeSelector from './LineageTabTimeSelector';
import { generateSchemaFieldUrn } from './utils';

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
}

export function LineageColumnView({ defaultDirection }: Props) {
    const { urn, entityType, entityData } = useEntityData();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(defaultDirection);
    const [selectedColumn, setSelectedColumn] = useState<string | undefined>(params?.column as string);
    const [shouldRefetch, setShouldRefetch] = useState(false);
    const [skipCache, setSkipCache] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
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
            <LineageTabContext.Provider value={{ isColumnLevelLineage, selectedColumn, lineageDirection }}>
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
