/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import DeleteOutlineOutlinedIcon from '@mui/icons-material/DeleteOutlineOutlined';
import React from 'react';
import styled from 'styled-components';

import { ViewItem } from '@app/entityV2/view/menu/item/ViewItem';

type Props = {
    key: string;
    onClick: () => void;
};

const DeleteOutlineOutlinedIconStyle = styled(DeleteOutlineOutlinedIcon)`
    font-size: 14px !important;
`;

/**
 * Delete a View Item
 */
export const DeleteViewItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem
            key={key}
            onClick={onClick}
            dataTestId="view-dropdown-delete"
            tip="Delete this View"
            title="Delete"
            icon={<DeleteOutlineOutlinedIconStyle />}
        />
    );
};
