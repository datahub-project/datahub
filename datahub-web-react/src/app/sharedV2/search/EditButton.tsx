import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';

const StyledButton = styled(Button)`
    font-size: 8px;
    padding-left: 12px;
    padding-right: 12px;
    background-color: ${SEARCH_COLORS.TITLE_PURPLE};
    color: #ffffff;
    :hover {
        background-color: #ffffff;
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        border: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};
    }
    height: 28px;
`;

type Props = {
    setShowSelectMode: (showSelectMode: boolean) => any;
    disabled?: boolean;
};

export default function EditButton({ setShowSelectMode, disabled }: Props) {
    return (
        <Tooltip title="Edit..." showArrow={false} placement="top">
            <StyledButton type="text" onClick={() => setShowSelectMode(true)} disabled={disabled}>
                <EditOutlined />
            </StyledButton>
        </Tooltip>
    );
}
