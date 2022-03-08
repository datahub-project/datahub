import React from 'react';
import { Image, Typography, Button } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';
import { formatNumber } from './formatNumber';

const Container = styled(Button)`
    margin-right: 12px;
    margin-left: 12px;
    margin-bottom: 12px;
    width: 160px;
    height: 140px;
    display: flex;
    justify-content: center;
    border-radius: 4px;
    align-items: center;
    flex-direction: column;
    border: 1px solid ${ANTD_GRAY[4]};
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
    white-space: unset;
`;

const PlatformLogo = styled(Image)`
    max-height: 32px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const CountText = styled(Typography.Text)`
    font-size: 18px;
    color: ${ANTD_GRAY[8]};
`;

const LogoContainer = styled.div``;

const TitleContainer = styled.div``;

const Title = styled(Typography.Title)`
    word-break: break-word;
`;

type Props = {
    logoUrl?: string;
    logoComponent?: React.ReactNode;
    name: string;
    count?: number;
    onClick?: () => void;
};

export const LogoCountCard = ({ logoUrl, logoComponent, name, count, onClick }: Props) => {
    return (
        <Container type="link" onClick={onClick}>
            <LogoContainer>
                {(logoUrl && <PlatformLogo preview={false} src={logoUrl} alt={name} />) || logoComponent}
            </LogoContainer>
            <TitleContainer>
                <Title
                    ellipsis={{
                        rows: 4,
                    }}
                    level={5}
                >
                    {name}
                </Title>
            </TitleContainer>
            {count && <CountText>{formatNumber(count)}</CountText>}
        </Container>
    );
};
