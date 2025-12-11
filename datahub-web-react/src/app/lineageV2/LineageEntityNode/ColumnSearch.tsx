/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Input } from 'antd';
import React, { Dispatch, SetStateAction } from 'react';
import styled from 'styled-components';

import { onClickPreventSelect } from '@app/lineageV2/common';

const SearchInput = styled(Input)`
    border-radius: 4px;
    border: 0.5px solid #d9d9d9;
    cursor: text;
    font-size: 10px;
    height: 22px;
    padding: 8px;
    width: 100%;

    :focus,
    :hover {
        border: 0.5px solid #1890ff;
        box-shadow: none;
        outline: none;
    }
`;

interface Props {
    searchText: string;
    setSearchText: Dispatch<SetStateAction<string>>;
}

export default function ColumnSearch({ searchText, setSearchText }: Props) {
    // Add nodrag class to prevent node from being selected on click
    // See https://reactflow.dev/api-reference/types/node-props#notes
    return (
        <SearchInput
            defaultValue={searchText}
            placeholder="Find column"
            onChange={(e) => setSearchText(e.target.value.trim())}
            onClick={onClickPreventSelect}
        />
    );
}
