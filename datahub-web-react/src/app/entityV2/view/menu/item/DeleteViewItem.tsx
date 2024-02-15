import React from 'react';
import styled from 'styled-components';
import DeleteOutlineOutlinedIcon from '@mui/icons-material/DeleteOutlineOutlined';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

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
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-delete">
            <IconItemTitle tip="Delete this View" title="Delete" icon={<DeleteOutlineOutlinedIconStyle />} />
        </Menu.Item>
    );
};
