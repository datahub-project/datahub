import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { CheckOutlined, LinkOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import { StyledMenuItem } from '../styledComponents';

interface CopyLinkMenuItemProps {
    key: string;
}

const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
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
                <TextSpan>
                    <b>Copy Link</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
