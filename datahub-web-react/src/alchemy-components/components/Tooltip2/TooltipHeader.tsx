import colors from '@src/alchemy-components/theme/foundations/colors';
import React from 'react';
import styled from 'styled-components';
import { TooltipHeaderProps } from './types';

const Container = styled.div`
    display: flex;
    align-items: center;
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
    min-width: 0;
    margin-right: 8px;
`;

const PrimaryTitle = styled.div`
    display: flex;
    align-items: center;
`;

const Title = styled.div`
    font-weight: 500;
    font-size: 14px;
    color: ${colors.gray[600]};
`;

const TitleSuffix = styled.div`
    margin-left: 4px;
`;

export const SubTitle = styled.div`
    font-weight: 400;
    font-size: 12px;
    flex-shrink: 1;
    min-width: 0;
    color: ${colors.gray[1700]};
`;

const ActionContainer = styled.div`
    margin-left: auto;
    flex-shrink: 0;
`;

const Image = styled.img`
    width: 32px;
    height: 32px;
    border-radius: 200px;
    margin-right: 8px;
    flex-shrink: 0;
    object-fit: contain;
`;

export function TooltipHeader({ title, subTitle, image, action: Action, titleSuffix }: TooltipHeaderProps) {
    if (!title) return null;

    return (
        <Container>
            {image && <Image src={image} alt="Tooltip header" />}
            <TitleContainer>
                <PrimaryTitle>
                    <Title>{title}</Title>
                    {titleSuffix && <TitleSuffix>{titleSuffix}</TitleSuffix>}
                </PrimaryTitle>
                {subTitle && <SubTitle>{subTitle}</SubTitle>}
            </TitleContainer>
            {Action && (
                <ActionContainer>
                    <Action />
                </ActionContainer>
            )}
        </Container>
    );
}

export default TooltipHeader;
