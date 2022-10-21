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
    isShowingColumnSelect: boolean;
    setSelectedColumn: (column: any) => void;
    setIsShowingColumnSelect: (isShowing: boolean) => void;
}

export default function ColumnsLineageSelect({
    selectedColumn,
    isShowingColumnSelect,
    setSelectedColumn,
    setIsShowingColumnSelect,
}: Props) {
    const { entityData } = useEntityData();
    const location = useLocation();
    const history = useHistory();

    function selectColumn(column: any) {
        updateQueryParams({ column }, location, history);
        setSelectedColumn(column);
    }

    const columnButtonTooltip = isShowingColumnSelect ? 'Hide column level lineage' : 'Show column level lineage';

    return (
        <>
            {isShowingColumnSelect && (
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
                <Button type="text" onClick={() => setIsShowingColumnSelect(!isShowingColumnSelect)}>
                    <ImpactAnalysisIcon />
                </Button>
            </Tooltip>
        </>
    );
}
