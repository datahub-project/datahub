import React, { useState } from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import EmailShare from 'react-email-share-link';
import MenuItem from 'antd/lib/menu/MenuItem';
import { ANTD_GRAY } from '../../../entity/shared/constants';

interface EmailMenuItemProps {
    urn: string;
    name: string;
    type: string;
    key: string;
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

export default function EmailMenuItem({ urn, name, type, key }: EmailMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);
    const linkText = window.location.href;

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                navigator.clipboard.writeText(urn);
                setIsClicked(true);
            }}
        >
            <EmailShare subject={`${name} | ${type}`} body={`Check out this ${type} on DataHub: ${linkText}`}>
                {(link) => (
                    <Tooltip title={`Share this ${type} via email`}>
                        {isClicked ? <CheckOutlined /> : <MailOutlined />}
                        <TextSpan>
                            <a href={link} style={{ color: 'inherit' }}>
                                <b>Email</b>
                            </a>
                        </TextSpan>
                    </Tooltip>
                )}
            </EmailShare>
        </StyledMenuItem>
    );
}
