import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { UnionType } from '@app/searchV2/utils/constants';

type Props = {
    unionType: UnionType;
    onUpdate: (newValue: UnionType) => void;
    disabled?: boolean;
};

const { Option } = Select;

const StyledSelect = styled(Select)`
    border-radius: 5px;
    background: ${(props) => props.theme.colors.bgSurface};
    :hover {
        background: ${(props) => props.theme.colors.bgSurface};
    }
`;

export const AdvancedSearchFilterOverallUnionTypeSelect = ({ unionType, onUpdate, disabled = false }: Props) => {
    return (
        <>
            <StyledSelect
                showArrow={false}
                bordered={false}
                disabled={disabled}
                // these values are just for display purposes- the actual value is the unionType prop
                value={unionType === UnionType.AND ? 'all filters' : 'any filter'}
                onChange={(newValue) => {
                    if ((newValue as any) !== unionType) {
                        onUpdate(newValue as any);
                    }
                }}
                size="small"
                dropdownMatchSelectWidth={false}
            >
                <Option value={UnionType.AND}>all filters</Option>
                <Option value={UnionType.OR}>any filter</Option>
            </StyledSelect>
        </>
    );
};
