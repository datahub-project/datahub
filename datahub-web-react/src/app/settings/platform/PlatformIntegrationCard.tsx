import { Button, Divider, Image, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    width: 100%;
`;

const PlatformLogo = styled(Image)`
    max-height: 28px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const ImageContainer = styled.div``;

const NameContainer = styled.div`
    font-size: 14px;
`;

const TitleContainer = styled.div``;

const HeaderContainer = styled.div`
    margin-left: 16px;
    display: flex;
    width: 100%;
    justify-content: space-between;
    align-items: center;
`;

const IntegrationButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

type Props = {
    name: string;
    description: string;
    img: string;
    onClick: () => void;
};

export const PlatformIntegrationItem = ({ name, description, img, onClick }: Props) => {
    return (
        <span style={{ width: '100%' }}>
            <Container onClick={onClick}>
                <ImageContainer>
                    <PlatformLogo preview={false} src={img} alt={name} />
                </ImageContainer>
                <HeaderContainer>
                    <TitleContainer>
                        <IntegrationButton type="link">
                            <NameContainer>{name}</NameContainer>
                        </IntegrationButton>
                        <Typography.Paragraph type="secondary">{description}</Typography.Paragraph>
                    </TitleContainer>
                </HeaderContainer>
            </Container>
            <Divider />
        </span>
    );
};
