import React, { ReactNode } from 'react';
import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components/macro';

const ExternalUrlWrapper = styled.span`
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
        <ExternalUrlWrapper>
            <StyledButton type="link" href={href} target="_blank" rel="noreferrer noopener" onClick={onClick}>
                {children} <ArrowRightOutlined style={{ fontSize: 12 }} />
            </StyledButton>
        </ExternalUrlWrapper>
    );
}
