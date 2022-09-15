import { Select } from 'antd';
import React from 'react';

import { ANTD_GRAY } from '../entity/shared/constants';
import { UnionType } from './utils/constants';

type Props = {
    unionType: UnionType;
    onUpdate: (newValue: UnionType) => void;
};

const { Option } = Select;

export const AdvancedSearchFilterOverallUnionTypeSelect = ({ unionType, onUpdate }: Props) => {
    return (
        <>
            <Select
                style={{
                    background: ANTD_GRAY[4],
                    borderRadius: 5,
                    width: 75,
                }}
                showArrow={false}
                bordered={false}
                value={unionType === UnionType.AND ? 'all filters' : 'any filter'}
                onChange={(newValue) => {
                    if ((newValue as any) !== unionType) {
                        onUpdate(newValue as any);
                    }
                }}
                size="small"
            >
                <Option value={UnionType.AND}>all filters</Option>
                <Option value={UnionType.OR}>any filter</Option>
            </Select>
        </>
    );
};
