import { Tooltip } from '@components';
import React, { useEffect, useRef, useState } from 'react';

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
    iconAlignment: 'horizontal',
    isEmpty: false,
    isCardClickable: true,
};

export const Card = ({
    title,
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
    isCardClickable = cardDefaults.isCardClickable,
    dataTestId,
    noOfSubtitleLines,
    iconStyles,
    pillLabel,
}: CardProps) => {
    const subtitleRef = useRef<HTMLDivElement>(null);
    const [showSubtitleTooltip, setShowSubtitleTooltip] = useState(false);

    useEffect(() => {
        const element = subtitleRef.current;
        if (!element) return;

        requestAnimationFrame(() => {
            const isOverflowing = element.scrollHeight > element.clientHeight;
            setShowSubtitleTooltip(isOverflowing);
        });
    }, []);

    const subtitleElement = (
        <SubTitle ref={subtitleRef} $noOfSubtitleLines={noOfSubtitleLines}>
            {subTitle}
        </SubTitle>
    );

    return (
        <>
            {isEmpty ? (
                <CardContainer maxWidth={maxWidth} height={height} width={width} data-testid={dataTestId}>
                    <TitleContainer data-testid="no-data">
                        <Title $isEmpty={isEmpty}>No Data</Title>
                        <SubTitle>{subTitle}</SubTitle>
                    </TitleContainer>
                </CardContainer>
            ) : (
                <CardContainer
                    isClickable={(!!button || onClick) && isCardClickable}
                    onClick={onClick}
                    maxWidth={maxWidth}
                    height={height}
                    width={width}
                    style={style}
                    data-testid={dataTestId}
                >
                    {title && (
                        <Header iconAlignment={iconAlignment}>
                            <div style={iconStyles}>{icon}</div>

                            <TitleContainer>
                                <Title data-testid="title">
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
                                    {!!pillLabel && (
                                        <Pill label={pillLabel} size="sm" color="primary" clickable={false} />
                                    )}
                                </Title>
                                <SubTitleContainer>
                                    {showSubtitleTooltip ? (
                                        <Tooltip title={subTitle}>{subtitleElement}</Tooltip>
                                    ) : (
                                        subtitleElement
                                    )}
                                </SubTitleContainer>
                            </TitleContainer>

                            {button}
                        </Header>
                    )}
                    {children}
                </CardContainer>
            )}
        </>
    );
};
