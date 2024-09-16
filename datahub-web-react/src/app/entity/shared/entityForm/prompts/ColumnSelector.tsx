import { Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetEntityWithSchema } from '../../../../entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import { SchemaField } from '../../../../../types.generated';
import { downgradeV2FieldPath } from '../../../../entityV2/dataset/profile/schema/utils/utils';
import MultiSelectSelector from '../../components/styled/MultiSelectSelector';

const ColumnSelectorWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 8px 0 -8px 0;
`;

const StyledCheckbox = styled(Checkbox)`
    display: flex;
    align-items: flex-start;
`;

const StyledDivider = styled.div`
    color: '#373D44';
    font-family: Manrope;
    font-size: 14px;
    font-style: normal;
    font-weight: 600;
    line-height: 18px;
    display: flex;
`;

interface Props {
    field: SchemaField;
    isBulkApplyingFieldPath: boolean;
    selectedFieldPaths: string[];
    setIsBulkApplyingFieldPath: (isBulkApply: boolean) => void;
    setSelectedFieldPaths: (fieldPaths: string[]) => void;
    schemaFields?: SchemaField[];
}

export default function ColumnSelector({
    field,
    isBulkApplyingFieldPath,
    setIsBulkApplyingFieldPath,
    selectedFieldPaths,
    setSelectedFieldPaths,
    schemaFields,
}: Props) {
    const { entityWithSchema } = useGetEntityWithSchema(!!schemaFields?.length);
    const fields = schemaFields?.length ? schemaFields : entityWithSchema?.schemaMetadata?.fields;
    const allFieldsExceptCurrent =
        (fields as any)?.filter((obj) => obj.fieldPath !== field.fieldPath).map((f) => f.fieldPath) || [];

    const options = allFieldsExceptCurrent.map((f) => ({ value: f, label: downgradeV2FieldPath(f) || '' }));

    return (
        <ColumnSelectorWrapper>
            <StyledCheckbox
                onChange={() => setIsBulkApplyingFieldPath(!isBulkApplyingFieldPath)}
                checked={isBulkApplyingFieldPath}
            />
            <StyledDivider>Set the same response for</StyledDivider>
            <MultiSelectSelector
                options={options}
                selectedValues={selectedFieldPaths}
                setSelectedValues={setSelectedFieldPaths}
                isDisabled={!isBulkApplyingFieldPath}
                placeholder="Select fields"
                valuesLabel="fields"
                searchPlaceholder="Find fields"
            />
        </ColumnSelectorWrapper>
    );
}
