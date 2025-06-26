import { Image, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { HomePageButton } from '@app/shared/components';
import { formatNumber } from '@app/shared/formatNumber';

const PlatformLogo = styled(Image)`
    max-height: 32px;
    height: 32px;
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
        <HomePageButton type="link" onClick={onClick}>
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
            {count !== undefined && <CountText>{formatNumber(count)}</CountText>}
        </HomePageButton>
    );
};
