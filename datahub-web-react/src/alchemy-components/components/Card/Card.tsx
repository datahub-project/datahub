import React from 'react';

import {
    CardContainer,
    Header,
    SubTitle,
    SubTitleContainer,
    Title,
    TitleContainer,
} from '@components/components/Card/components';
import { CardProps } from '@components/components/Card/types';
import { Pill } from '@components/components/Pills';

export const cardDefaults: CardProps = {
    title: 'Title',
    iconAlignment: 'horizontal',
    isEmpty: false,
};

export const Card = ({
    title = cardDefaults.title,
    iconAlignment = cardDefaults.iconAlignment,
    subTitle,
    percent,
    button,
    onClick,
    icon,
    children,
    width,
    maxWidth,
    height,
    isEmpty,
    style,
}: CardProps) => {
    return (
        <>
            {isEmpty ? (
                <CardContainer maxWidth={maxWidth} height={height} width={width}>
                    <TitleContainer>
                        <Title $isEmpty={isEmpty}>No Data</Title>
                        <SubTitle>{subTitle}</SubTitle>
                    </TitleContainer>
                </CardContainer>
            ) : (
                <CardContainer
                    hasButton={!!button}
                    onClick={onClick}
                    maxWidth={maxWidth}
                    height={height}
                    width={width}
                    style={style}
                >
                    <Header iconAlignment={iconAlignment}>
                        {icon}
                        <TitleContainer>
                            <Title>
                                {title}
                                {!!percent && (
                                    <Pill
                                        label={`${Math.abs(percent)}%`}
                                        size="sm"
                                        color={percent < 0 ? 'red' : 'green'}
                                        leftIcon={percent < 0 ? 'TrendingDown' : 'TrendingUp'}
                                        clickable={false}
                                    />
                                )}
                            </Title>
                            <SubTitleContainer>
                                <SubTitle>{subTitle}</SubTitle>
                            </SubTitleContainer>
                        </TitleContainer>
                        {button}
                    </Header>
                    {children}
                </CardContainer>
            )}
        </>
    );
};
