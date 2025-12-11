/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { ReactNode } from 'react';
import styled from 'styled-components/macro';

const UrlButtonContainer = styled.span`
    font-size: 12px;
`;

const StyledButton = styled(Button)`
    > :hover {
        text-decoration: underline;
    }
    &&& {
        padding-bottom: 0px;
    }
    padding-left: 12px;
    padding-right: 12px;
`;

interface Props {
    href: string;
    children: ReactNode;
    onClick?: () => void;
}

const NOOP = () => {};

export default function UrlButton({ href, children, onClick = NOOP }: Props) {
    return (
        <UrlButtonContainer>
            <StyledButton type="link" href={href} target="_blank" rel="noreferrer noopener" onClick={onClick}>
                {children} <ArrowRightOutlined style={{ fontSize: 12 }} />
            </StyledButton>
        </UrlButtonContainer>
    );
}
