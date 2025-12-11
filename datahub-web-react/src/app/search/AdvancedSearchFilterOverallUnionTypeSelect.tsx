/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { UnionType } from '@app/search/utils/constants';

type Props = {
    unionType: UnionType;
    onUpdate: (newValue: UnionType) => void;
    disabled?: boolean;
};

const { Option } = Select;

const StyledSelect = styled(Select)`
    border-radius: 5px;
    background: ${ANTD_GRAY[4]};
    :hover {
        background: ${ANTD_GRAY[4.5]};
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
