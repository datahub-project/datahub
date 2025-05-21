import { PlusOutlined } from '@ant-design/icons';
import { Icon } from '@components';
import { Button, Input, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { ADD_UNAUTHORIZED_MESSAGE } from '@app/entity/shared/tabs/Dataset/Queries/utils/constants';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
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
                <Button disabled={addQueryDisabled} type="text" onClick={onAddQuery} data-testid="add-query-button">
                    <PlusOutlined /> Add Query
                </Button>
            </Tooltip>
            <StyledInput
                placeholder="Search in queries..."
                onChange={onChangeSearch}
                allowClear
                prefix={<Icon icon="MagnifyingGlass" source="phosphor" />}
                data-testid="search-query-input"
            />
        </TabToolbar>
    );
}
