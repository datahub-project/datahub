import { Icon, Text, Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import {
    FieldLabel,
    FlexContainer,
    ItemsContainer,
    RowContainer,
    StyledIcon,
    ValueListContainer,
    ValueType,
    ValuesList,
    VerticalDivider,
} from '@app/govern/structuredProperties/styledComponents';
import { PropValueField, isStringOrNumberTypeSelected } from '@app/govern/structuredProperties/utils';
import { AllowedValue } from '@src/types.generated';

// Allowed-value entries in this admin form are stored flat as { stringValue|numberValue, description }
// rather than the canonical AllowedValue { value: PropertyValue, ... } shape. Sort by the flat field directly.
const ALLOWED_VALUES_COLLATOR = new Intl.Collator(undefined, { sensitivity: 'base', numeric: true });

interface Props {
    selectedValueType: string;
    allowedValues: AllowedValue[] | undefined;
    valueField: PropValueField;
    setShowAllowedValuesDrawer: React.Dispatch<React.SetStateAction<boolean>>;
}

const AllowedValuesField = ({ selectedValueType, allowedValues, valueField, setShowAllowedValuesDrawer }: Props) => {
    const { t } = useTranslation('governance.structured-properties');
    const valueType = valueField === 'stringValue' ? t('valueType.text') : t('valueType.number');

    return (
        <>
            {isStringOrNumberTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <FlexContainer>
                            {t('allowedValues.title')}
                            <Tooltip title={t('allowedValues.fieldTooltip')} showArrow={false}>
                                <Icon icon={Info} color="iconBrand" size="lg" />
                            </Tooltip>
                        </FlexContainer>
                    </FieldLabel>

                    {allowedValues && allowedValues.length > 0 ? (
                        <ItemsContainer>
                            <ValuesList>
                                {[...allowedValues]
                                    .sort((a, b) =>
                                        ALLOWED_VALUES_COLLATOR.compare(
                                            String(a[valueField] ?? ''),
                                            String(b[valueField] ?? ''),
                                        ),
                                    )
                                    .map((val, index, arr) => {
                                        return (
                                            <>
                                                <Text>{val[valueField]}</Text>
                                                {index < arr.length - 1 && <VerticalDivider type="vertical" />}
                                            </>
                                        );
                                    })}
                            </ValuesList>
                            <Tooltip title={t('allowedValues.updateTooltip')} showArrow={false}>
                                <StyledIcon
                                    icon={CaretRight}
                                    color="gray"
                                    onClick={() => setShowAllowedValuesDrawer(true)}
                                />
                            </Tooltip>
                        </ItemsContainer>
                    ) : (
                        <ValueListContainer>
                            <Trans
                                t={t}
                                i18nKey="allowedValues.anyValueAllowed"
                                components={{ type: <ValueType /> }}
                                values={{ valueType }}
                            />
                            <Tooltip title={t('allowedValues.updateTooltip')} showArrow={false}>
                                <Icon icon={Plus} color="gray" onClick={() => setShowAllowedValuesDrawer(true)} />
                            </Tooltip>
                        </ValueListContainer>
                    )}
                </RowContainer>
            )}
        </>
    );
};

export default AllowedValuesField;
