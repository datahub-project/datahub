import { PlusOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import * as React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DEGREE_FILTER_NAME, ORDERED_FIELDS, getFieldToLabel } from '@app/search/utils/constants';

import { FacetFilterInput } from '@types';

const StyledPlus = styled(PlusOutlined)`
    margin-right: 6px;
`;

interface Props {
    selectedFilters: Array<FacetFilterInput>;
    onFilterFieldSelect: (value) => void;
    isCompact?: boolean;
}

const { Option } = Select;

export const AdvancedSearchAddFilterSelect = ({ selectedFilters, onFilterFieldSelect, isCompact }: Props) => {
    const { t } = useTranslation('search');
    const fieldToLabel = getFieldToLabel();
    const selectStyle = {
        padding: isCompact ? 3 : 6,
        fontWeight: 500,
        width: 'auto',
        paddingTop: isCompact ? 0 : undefined,
        paddingBottom: isCompact ? 0 : undefined,
    };

    return (
        <Select
            data-testid="adv-search-add-filter-select"
            value={{
                // eslint-disable-next-line i18next/no-literal-string
                value: 'value',
                label: (
                    <div>
                        <StyledPlus />
                        {t('advancedSearch.addFilter')}
                    </div>
                ),
            }}
            labelInValue
            style={selectStyle}
            onChange={onFilterFieldSelect}
            dropdownMatchSelectWidth={false}
            filterOption={(_, option) => option?.value === 'null'}
        >
            {ORDERED_FIELDS.filter((key) => key !== DEGREE_FILTER_NAME).map((key) => (
                <Option
                    // disable the `entity` option if they already have an entity filter selected
                    disabled={key === 'entity' && !!selectedFilters.find((filter) => filter.field === 'entity')}
                    value={key}
                    data-testid={`adv-search-add-filter-${key}`}
                    key={key}
                >
                    {fieldToLabel[key]}
                </Option>
            ))}
        </Select>
    );
};
