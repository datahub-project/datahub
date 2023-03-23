import React from 'react';
import styled from 'styled-components/macro';
import { Button, Typography } from 'antd';

const FooterContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-end;
    margin-top: 16px;
    margin-right: 24px;
    margin-bottom: 16px;
    gap: 8px;
`;

const FooterButtonLabel = styled(Typography.Text)<{ color: string }>`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
    color: ${({ color }) => color};
`;

interface Props {
    onClose: () => void;
}

export default function Footer({ onClose }: Props) {
    return (
        <FooterContainer>
            <Button onClick={onClose}>
                <FooterButtonLabel color="rgba(0, 0, 0, 0.88)">Cancel</FooterButtonLabel>
            </Button>
            <Button type="primary" onClick={onClose}>
                <FooterButtonLabel color="white">Subscribe</FooterButtonLabel>
            </Button>
        </FooterContainer>
    );
}
