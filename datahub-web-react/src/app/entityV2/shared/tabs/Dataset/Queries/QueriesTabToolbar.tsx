import { Button, Input, Tooltip } from 'antd';
import { PlusOutlined, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React from 'react';
import TabToolbar from '../../../components/styled/TabToolbar';
import { ADD_UNAUTHORIZED_MESSAGE } from './utils/constants';
import { REDESIGN_COLORS } from '../../../constants';

const StyledInput = styled(Input)`
    border-radius: 5px;
    max-width: 300px;
`;

const PrimaryButton = styled(Button)`
    color: #ffffff;
    font-size: 12px;
    border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    margin-left: 8px;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

type Props = {
    addQueryDisabled: boolean;
    onAddQuery: () => void;
    onChangeSearch: (text: any) => void;
};

export default function QueriesTabToolbar({ addQueryDisabled, onAddQuery, onChangeSearch }: Props) {
    return (
        <TabToolbar>
            <Tooltip
                placement="right"
                title={(addQueryDisabled && ADD_UNAUTHORIZED_MESSAGE) || 'Add a highlighted query'}
            >
                <PrimaryButton
                    disabled={addQueryDisabled}
                    type="text"
                    onClick={onAddQuery}
                    data-testid="add-query-button"
                >
                    <PlusOutlined /> Add Query
                </PrimaryButton>
            </Tooltip>
            <StyledInput
                placeholder="Search in queries..."
                onChange={onChangeSearch}
                allowClear
                prefix={<SearchOutlined />}
                data-testid="search-query-input"
            />
        </TabToolbar>
    );
}
