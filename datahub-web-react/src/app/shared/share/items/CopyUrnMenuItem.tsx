import React, { useState } from 'react';
import styled from 'styled-components';
import { CheckOutlined, CopyOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import MenuItem from 'antd/lib/menu/MenuItem';
import { ANTD_GRAY } from '../../../entity/shared/constants';

interface CopyUrnMenuItemProps {
    urn: string;
    key: string;
    type: string;
}

const StyledMenuItem = styled(MenuItem)`
    && {
        color: ${ANTD_GRAY[8]};
    }
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
                <span>
                    <b>Copy URN</b>
                </span>
            </Tooltip>
        </StyledMenuItem>
    );
}
