import React, { useState } from 'react';
import styled from 'styled-components';
import {t} from "i18next";
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
        background-color: ${ANTD_GRAY[1]};
    }
`;

const TextSpan = styled.span`
    padding-left: 12px;
`;

export default function CopyUrnMenuItem({ urn, key, type }: CopyUrnMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);
    const CopyTitleText = `${t("Copy_Urn_Title", {type})}`;

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                navigator.clipboard.writeText(urn);
                setIsClicked(true);
            }}
        >
            <Tooltip title={CopyTitleText}>
                {isClicked ? <CheckOutlined /> : <CopyOutlined />}
                <TextSpan>
                    <b>{t ('Copy URN') }</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
