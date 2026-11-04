import { SimpleSelect } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { UnionType } from '@app/searchV2/utils/constants';

type Props = {
    unionType: UnionType;
    onUpdate: (newValue: UnionType) => void;
    disabled?: boolean;
};

// Renders inline inside a translated sentence, so the select is kept inline-sized.
const InlineSelectWrapper = styled.span`
    display: inline-flex;
    vertical-align: middle;
`;

const SelectBase = styled.span<{ $disabled: boolean }>`
    display: inline-block;
    padding: 0 7px;
    border-radius: 5px;
    background: ${(props) => props.theme.colors.bgSurface};
    cursor: ${(props) => (props.$disabled ? 'not-allowed' : 'pointer')};
    :hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

export const AdvancedSearchFilterOverallUnionTypeSelect = ({ unionType, onUpdate, disabled = false }: Props) => {
    const { t } = useTranslation('search');
    const options = [
        { value: UnionType.AND.toString(), label: t('advancedSearch.allFilters') },
        { value: UnionType.OR.toString(), label: t('advancedSearch.anyFilter') },
    ];
    const selectedLabel = unionType === UnionType.AND ? t('advancedSearch.allFilters') : t('advancedSearch.anyFilter');

    return (
        <InlineSelectWrapper>
            <SimpleSelect
                options={options}
                values={[unionType.toString()]}
                isDisabled={disabled}
                width="fit-content"
                size="sm"
                showClear={false}
                onUpdate={(newValues) => {
                    const newValue = Number(newValues[0]) as UnionType;
                    if (newValues.length && newValue !== unionType) {
                        onUpdate(newValue);
                    }
                }}
                renderSelectBase={({ onClick }) => (
                    <SelectBase $disabled={disabled} onClick={disabled ? undefined : onClick}>
                        {selectedLabel}
                    </SelectBase>
                )}
            />
        </InlineSelectWrapper>
    );
};
