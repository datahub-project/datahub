import { Button, Select, Tooltip } from 'antd';
import * as React from 'react';
import styled from 'styled-components/macro';
import { blue } from '@ant-design/colors';
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

const StyledButton = styled(Button)<{ isSelected: boolean }>`
    transition: color 0s;
    display: flex;
    align-items: center;

    ${(props) =>
        props.isSelected &&
        `
        color: ${blue[5]};
        &:focus, &:hover {
            color: ${blue[5]};
        }
    `};
`;

const TextWrapper = styled.span`
    margin-left: 8px;
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
                <StyledButton
                    type="text"
                    onClick={() => setIsColumnLevelLineage(!isColumnLevelLineage)}
                    data-testid="column-lineage-toggle"
                    isSelected={isColumnLevelLineage}
                >
                    <ImpactAnalysisIcon />
                    <TextWrapper>Column Lineage</TextWrapper>
                </StyledButton>
            </Tooltip>
        </>
    );
}
