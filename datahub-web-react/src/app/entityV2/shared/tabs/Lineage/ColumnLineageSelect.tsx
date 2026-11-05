import { Button, SimpleSelect, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import * as React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';
import updateQueryParams from '@app/sharedV2/updateQueryParams';

const StyledButton = styled(Button)<{ $isSelected: boolean }>`
    && {
        transition: color 0s;
        display: inline-flex;
        align-items: center;
        gap: 4px;
        height: auto;
        padding: 4px 8px;
        font-size: 12px;
        font-weight: 700;
        line-height: 16px;
        color: ${(props) => (props.$isSelected ? props.theme.colors.textBrand : props.theme.colors.text)};
    }
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
    const { t } = useTranslation('lineage');
    const { entityData } = useEntityData();
    const location = useLocation();
    const history = useHistory();
    const { entityWithSchema } = useGetEntityWithSchema();

    function selectColumn(column: any) {
        updateQueryParams({ column }, location, history);
        setSelectedColumn(column);
    }

    // Schema fields and input fields can overlap; keep one option per field path
    const columnOptions = React.useMemo(() => {
        const fieldPaths = [
            ...(entityWithSchema?.schemaMetadata?.fields?.map((field) => field.fieldPath) || []),
            ...(entityData?.inputFields?.fields?.map((field) => field?.schemaField?.fieldPath || '') || []),
        ];
        return Array.from(new Set(fieldPaths.filter(Boolean))).map((fieldPath) => ({
            value: fieldPath,
            label: downgradeV2FieldPath(fieldPath) || fieldPath,
        }));
    }, [entityWithSchema, entityData]);

    const columnButtonTooltip = isColumnLevelLineage ? t('columnLineage.hideTooltip') : t('columnLineage.showTooltip');

    return (
        <>
            {isColumnLevelLineage && (
                <SimpleSelect
                    dataTestId="column-selector-dropdown"
                    options={columnOptions}
                    values={selectedColumn ? [selectedColumn] : []}
                    onUpdate={(values) => selectColumn(values[0])}
                    showSearch
                    showClear
                    placeholder={t('columnLineage.selectColumnPlaceholder')}
                    optionDataTestId={(option) => `column-option-${option.label}`}
                    renderCustomOptionText={(option) => (
                        <Tooltip title={option.label} showArrow={false}>
                            <span>{option.label}</span>
                        </Tooltip>
                    )}
                    width="fit-content"
                    minWidth="200px"
                    maxWidth={300}
                />
            )}
            <Tooltip title={columnButtonTooltip} showArrow={false}>
                <StyledButton
                    variant="text"
                    onClick={() => setIsColumnLevelLineage(!isColumnLevelLineage)}
                    data-testid="column-lineage-toggle"
                    $isSelected={isColumnLevelLineage}
                >
                    <TreeStructure size={16} />
                    <Trans i18nKey="lineage:columnLineage.buttonLabel" components={{ b: <b /> }} />
                    <CaretDown size={12} />
                </StyledButton>
            </Tooltip>
        </>
    );
}
