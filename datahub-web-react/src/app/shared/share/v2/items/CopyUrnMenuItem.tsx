import React, { useState } from 'react';
import styled from 'styled-components';
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { StyledMenuItem } from '../styledComponents';

interface CopyUrnMenuItemProps {
    urn: string;
    key: string;
    type: string;
}

const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
`;

export default function CopyUrnMenuItem({ urn, key, type }: CopyUrnMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                navigator.clipboard.writeText(urn);
                setIsClicked(true);
            }}
        >
            <Tooltip title={`Copy the URN for this ${type}. An URN uniquely identifies an entity on DataHub.`}>
                {isClicked ? <CheckOutlined /> : <CopyOutlined />}
                <TextSpan>
                    <b>Copy URN</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
