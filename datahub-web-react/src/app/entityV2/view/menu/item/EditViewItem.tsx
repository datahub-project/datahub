/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import React from 'react';
import styled from 'styled-components';

import { ViewItem } from '@app/entityV2/view/menu/item/ViewItem';

const EditOutlinedIconStyled = styled(EditOutlinedIcon)`
    font-size: 14px !important;
`;

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Edit View Menu Item
 */
export const EditViewItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem
            key={key}
            onClick={onClick}
            dataTestId="view-dropdown-edit"
            tip="Edit this View"
            title="Edit"
            icon={<EditOutlinedIconStyled />}
        />
    );
};
