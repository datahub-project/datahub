import { Button, Select, Tooltip } from 'antd';
import * as React from 'react';
import styled from 'styled-components/macro';
import { useHistory, useLocation } from 'react-router';
import { ImpactAnalysisIcon } from '../Dataset/Schema/components/MenuColumn';
import updateQueryParams from '../../../../shared/updateQueryParams';
import { downgradeV2FieldPath } from '../../../dataset/profile/schema/utils/utils';
import { useEntityData } from '../../EntityContext';

const StyledSelect = styled(Select)`
    margin-right: 5px;
    min-width: 140px;
    max-width: 200px;
`;

interface Props {
    selectedColumn?: string;
    isColumnLevelLineage: boolean;
    setSelectedColumn: (column: any) => void;
    setIsColumnLevelLineage: (isShowing: boolean) => void;
}

export default function ColumnsLineageSelect({
    selectedColumn,
    isColumnLevelLineage,
    setSelectedColumn,
    setIsColumnLevelLineage,
}: Props) {
    const { entityData } = useEntityData();
    const location = useLocation();
    const history = useHistory();

    function selectColumn(column: any) {
        updateQueryParams({ column }, location, history);
        setSelectedColumn(column);
    }

    const columnButtonTooltip = isColumnLevelLineage ? 'Hide column level lineage' : 'Show column level lineage';

    return (
        <>
            {isColumnLevelLineage && (
                <StyledSelect
                    value={selectedColumn}
                    onChange={selectColumn}
                    showSearch
                    allowClear
                    placeholder="Select column"
                >
                    {entityData?.schemaMetadata?.fields.map((field) => (
                        <Select.Option value={field.fieldPath}>{downgradeV2FieldPath(field.fieldPath)}</Select.Option>
                    ))}
                </StyledSelect>
            )}
            <Tooltip title={columnButtonTooltip}>
                <Button type="text" onClick={() => setIsColumnLevelLineage(!isColumnLevelLineage)}>
                    <ImpactAnalysisIcon />
                </Button>
            </Tooltip>
        </>
    );
}
