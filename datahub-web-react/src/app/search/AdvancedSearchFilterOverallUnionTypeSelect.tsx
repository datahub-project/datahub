import { Select } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { UnionType } from '@app/search/utils/constants';

type Props = {
    unionType: UnionType;
    onUpdate: (newValue: UnionType) => void;
    disabled?: boolean;
};

const { Option } = Select;

const StyledSelect = styled(Select)`
    border-radius: 5px;
    background: ${(props) => props.theme.colors.border};
    :hover {
        background: ${(props) => props.theme.colors.border};
    }
`;

export const AdvancedSearchFilterOverallUnionTypeSelect = ({ unionType, onUpdate, disabled = false }: Props) => {
    const { t } = useTranslation('search');
    return (
        <>
            <StyledSelect
                showArrow={false}
                bordered={false}
                disabled={disabled}
                // these values are just for display purposes- the actual value is the unionType prop
                value={unionType === UnionType.AND ? t('advancedSearch.allFilters') : t('advancedSearch.anyFilter')}
                onChange={(newValue) => {
                    if ((newValue as any) !== unionType) {
                        onUpdate(newValue as any);
                    }
                }}
                size="small"
                dropdownMatchSelectWidth={false}
            >
                <Option value={UnionType.AND}>{t('advancedSearch.allFilters')}</Option>
                <Option value={UnionType.OR}>{t('advancedSearch.anyFilter')}</Option>
            </StyledSelect>
        </>
    );
};
