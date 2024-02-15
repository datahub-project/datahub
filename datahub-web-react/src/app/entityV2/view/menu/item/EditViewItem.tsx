import React from 'react';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import styled from 'styled-components';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

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
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-edit">
            <IconItemTitle tip="Edit this View" title="Edit" icon={<EditOutlinedIconStyled />} />
        </Menu.Item>
    );
};
