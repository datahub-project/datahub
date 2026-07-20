import { Button, Image } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Container = styled(Button)`
    && {
        padding: 32px;
        height: 100%;
        width: 100%;
        display: flex;
        justify-content: center;
        border-radius: 8px;
        align-items: start;
        flex-direction: column;
        border: 1px solid ${(props) => props.theme.colors.border};
        background-color: ${(props) => props.theme.colors.bg};
        white-space: unset;
        position: relative;
        overflow: hidden;
    }
    &&:hover {
        border: 1px solid ${(props) => props.theme.colors.borderHover};
        background-color: ${(props) => props.theme.colors.bg};
    }
`;

const PlatformLogo = styled(Image)`
    max-height: 32px;
    height: 32px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const LogoContainer = styled.div`
    margin-bottom: 14px;
`;

const Title = styled.div`
    word-break: break-word;
    color: ${(props) => props.theme.colors.text};
    font-weight: bold;
    font-size: 16px;
    margin-bottom: 8px;
`;

const Description = styled.div`
    word-break: break-word;
    text-align: left;
    color: ${(props) => props.theme.colors.textTertiary};
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const InitialsAvatar = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 6px;
    background-color: ${(props) => props.theme.colors.bgSurfaceInfo};
    color: ${(props) => props.theme.colors.textInformation};
    font-weight: 700;
    font-size: 13px;
`;

function getInitials(name: string): string {
    return name
        .split(/[\s-]+/)
        .filter(Boolean)
        .slice(0, 2)
        .map((word) => word[0].toUpperCase())
        .join('');
}

type Props = {
    logoUrl?: string;
    logoComponent?: React.ReactNode;
    name: string;
    description?: string;
    onClick?: () => void;
    dataTestId?: string;
};

export const DataPlatformCard = ({ logoUrl, logoComponent, name, description, onClick, dataTestId }: Props) => {
    // For community plugins without a logo, show styled initials
    const fallbackLogo = !logoUrl && !logoComponent ? <InitialsAvatar>{getInitials(name)}</InitialsAvatar> : null;

    return (
        <Container type="link" onClick={onClick} data-testid={dataTestId}>
            <LogoContainer>
                {(logoUrl && <PlatformLogo preview={false} src={logoUrl} alt={name} />) ||
                    logoComponent ||
                    fallbackLogo}
            </LogoContainer>
            <Title>{name}</Title>
            <Description>{description}</Description>
        </Container>
    );
};
