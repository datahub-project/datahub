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
