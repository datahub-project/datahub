import React from 'react';
import { Image, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const Container = styled.div`
    margin-right: 24px;
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
`;

const PlatformLogo = styled(Image)`
    max-height: 36px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const PlatformContainer = styled.div``;

const PlatformTitleContainer = styled.div``;

type Props = {
    logoUrl: string;
    name: string;
};

export const PlatformCard = ({ logoUrl, name }: Props) => {
    return (
        <Container>
            <PlatformContainer>
                <PlatformLogo preview={false} src={logoUrl} alt={name} />
            </PlatformContainer>
            <PlatformTitleContainer>
                <Typography.Title level={4}>{name}</Typography.Title>
            </PlatformTitleContainer>
        </Container>
    );
};
