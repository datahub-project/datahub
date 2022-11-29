import React, { useState } from 'react';
import styled from 'styled-components';
import { CheckOutlined, LinkOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import MenuItem from 'antd/lib/menu/MenuItem';
import { ANTD_GRAY } from '../../../entity/shared/constants';

interface CopyLinkMenuItemProps {
    key: string;
}

const StyledMenuItem = styled(MenuItem)`
    && {
        color: ${ANTD_GRAY[8]};
    }
`;

const StyledLinkOutlined = styled(LinkOutlined)`
    font-size: 14px;
`;

export default function CopyLinkMenuItem({ key }: CopyLinkMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                navigator.clipboard.writeText(window.location.href);
                setIsClicked(true);
            }}
        >
            <Tooltip title="Copy a shareable link to this entity.">
                {isClicked ? <CheckOutlined /> : <StyledLinkOutlined />}
                <span>
                    <b>Copy Link</b>
                </span>
            </Tooltip>
        </StyledMenuItem>
    );
}
