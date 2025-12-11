/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

const EditQueryActionButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px 4px 0px 4px;
    }
`;

export type Props = {
    onClickEdit?: () => void;
    index?: number;
};

export default function QueryCardEditButton({ onClickEdit, index }: Props) {
    return (
        <EditQueryActionButton type="text" onClick={onClickEdit} data-testid={`query-edit-button-${index}`}>
            <EditOutlined />
        </EditQueryActionButton>
    );
}
