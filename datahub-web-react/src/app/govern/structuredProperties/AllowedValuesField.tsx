import { Icon, Tooltip } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import { useTranslation } from 'react-i18next';

import AllowedValuesList from '@app/govern/structuredProperties/AllowedValuesList';
import {
    AllowedValuesLabel,
    AllowedValuesRequired,
    AllowedValuesSection,
} from '@app/govern/structuredProperties/styledComponents';
import { AllowedValueRow, PropValueField, isStringOrNumberTypeSelected } from '@app/govern/structuredProperties/utils';

type Props = {
    selectedValueType: string;
    valueField: PropValueField;
    isReadOnly: boolean;
    rows: AllowedValueRow[];
    errors?: Record<string, string>;
    addRow: () => void;
    updateRow: (rowId: string, patch: Partial<AllowedValueRow>) => void;
    removeRow: (rowId: string) => void;
    moveRow: (from: number, to: number) => void;
};

const AllowedValuesField = ({
    selectedValueType,
    valueField,
    isReadOnly,
    rows,
    errors,
    addRow,
    updateRow,
    removeRow,
    moveRow,
}: Props) => {
    const { t } = useTranslation('governance.structured-properties');

    if (!isStringOrNumberTypeSelected(selectedValueType)) return null;

    // The list itself is optional, so the asterisk only applies once there are rows to fill in.
    const hasRows = !!rows.length;

    return (
        <AllowedValuesSection>
            <AllowedValuesLabel>
                {t('allowedValues.title')}
                {hasRows && <AllowedValuesRequired>*</AllowedValuesRequired>}
                <Tooltip title={t('allowedValues.fieldTooltip')} showArrow={false}>
                    <Icon icon={Info} size="md" />
                </Tooltip>
            </AllowedValuesLabel>
            <AllowedValuesList
                propType={valueField}
                isReadOnly={isReadOnly}
                rows={rows}
                errors={errors}
                addRow={addRow}
                updateRow={updateRow}
                removeRow={removeRow}
                moveRow={moveRow}
            />
        </AllowedValuesSection>
    );
};

export default AllowedValuesField;
