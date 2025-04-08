import { blue } from '@ant-design/colors';
import { CaretDownOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Select } from 'antd';
import * as React from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { ImpactAnalysisIcon } from '@app/entityV2/shared/tabs/Dataset/Schema/components/MenuColumn';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import updateQueryParams from '@app/shared/updateQueryParams';

const StyledSelect = styled(Select)`
    margin-right: 5px;
    min-width: 140px;
    max-width: 200px;
`;

const StyledButton = styled(Button)<{ $isSelected: boolean }>`
    transition: color 0s;
    display: flex;
    align-items: center;

    ${(props) =>
        props.$isSelected &&
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
    const { entityWithSchema } = useGetEntityWithSchema();

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
                    {entityWithSchema?.schemaMetadata?.fields?.map((field) => {
                        const fieldPath = downgradeV2FieldPath(field.fieldPath);
                        return (
                            <Select.Option value={field.fieldPath}>
                                <Tooltip title={fieldPath} showArrow={false}>
                                    {fieldPath}
                                </Tooltip>
                            </Select.Option>
                        );
                    })}
                    {entityData?.inputFields?.fields?.map((field, idx) => {
                        const fieldPath = downgradeV2FieldPath(field?.schemaField?.fieldPath);
                        const key = `${field?.schemaField?.fieldPath}-${idx}`;
                        return (
                            <Select.Option key={key} value={field?.schemaField?.fieldPath || ''}>
                                <Tooltip title={fieldPath} showArrow={false}>
                                    {fieldPath}
                                </Tooltip>
                            </Select.Option>
                        );
                    })}
                </StyledSelect>
            )}
            <Tooltip title={columnButtonTooltip} showArrow={false}>
                <StyledButton
                    type="text"
                    onClick={() => setIsColumnLevelLineage(!isColumnLevelLineage)}
                    data-testid="column-lineage-toggle"
                    $isSelected={isColumnLevelLineage}
                >
                    <ImpactAnalysisIcon />
                    <TextWrapper>
                        <b>Column Lineage</b>
                        <CaretDownOutlined style={{ fontSize: '10px', marginLeft: 4 }} />
                    </TextWrapper>
                </StyledButton>
            </Tooltip>
        </>
    );
}
