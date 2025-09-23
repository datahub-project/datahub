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
