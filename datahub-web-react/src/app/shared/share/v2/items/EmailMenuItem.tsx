import React, { useState } from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import qs from 'query-string';
import { StyledMenuItem } from '../styledComponents';

interface EmailMenuItemProps {
    urn: string;
    name: string;
    type: string;
    key: string;
}

const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
`;

export default function EmailMenuItem({ urn, name, type, key }: EmailMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const [isClicked, setIsClicked] = useState(false);
    const linkText = window.location.href;

    const link = qs.stringifyUrl({
        url: 'mailto:',
        query: {
            subject: `${name} | ${type}`,
            body: `Check out this ${type} on DataHub: ${linkText}. Urn: ${urn}`,
        },
    });

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                setIsClicked(true);
            }}
        >
            <Tooltip title={`Share this ${type} via email`}>
                {isClicked ? <CheckOutlined /> : <MailOutlined />}
                <TextSpan>
                    <a href={link} target="_blank" rel="noreferrer" style={{ color: 'inherit' }}>
                        <b>Email</b>
                    </a>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}
