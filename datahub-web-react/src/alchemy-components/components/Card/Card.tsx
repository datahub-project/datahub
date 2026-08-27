import { Icon, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { TrendDown } from '@phosphor-icons/react/dist/csr/TrendDown';
import { TrendUp } from '@phosphor-icons/react/dist/csr/TrendUp';
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import {
    CardContainer,
    CollapsibleBody,
    ExpandButton,
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
    size: 'md',
    collapsible: false,
    defaultExpanded: true,
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
    pill,
    size = cardDefaults.size,
    collapsible = cardDefaults.collapsible,
    defaultExpanded = cardDefaults.defaultExpanded,
    expanded,
    onExpandChange,
}: CardProps) => {
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');
    const subtitleRef = useRef<HTMLDivElement>(null);
    const [showSubtitleTooltip, setShowSubtitleTooltip] = useState(false);
    const [internalExpanded, setInternalExpanded] = useState(defaultExpanded ?? true);

    const isControlled = expanded !== undefined;
    const isExpanded = isControlled ? expanded : internalExpanded;
    const showBody = !collapsible || isExpanded;

    useEffect(() => {
        const element = subtitleRef.current;
        if (!element) return;

        requestAnimationFrame(() => {
            const isOverflowing = element.scrollHeight > element.clientHeight;
            setShowSubtitleTooltip(isOverflowing);
        });
    }, [showBody, subTitle]);

    const handleExpandToggle = (e?: React.MouseEvent) => {
        e?.stopPropagation();
        const next = !isExpanded;
        if (!isControlled) {
            setInternalExpanded(next);
        }
        onExpandChange?.(next);
    };

    const handleHeaderClick = (e: React.MouseEvent) => {
        if (!collapsible) return;
        handleExpandToggle(e);
    };

    const subtitleElement = (
        <SubTitle ref={subtitleRef} $noOfSubtitleLines={noOfSubtitleLines} $size={size}>
            {subTitle}
        </SubTitle>
    );

    const subtitleBlock = subTitle ? (
        <SubTitleContainer>
            {showSubtitleTooltip ? <Tooltip title={subTitle}>{subtitleElement}</Tooltip> : subtitleElement}
        </SubTitleContainer>
    ) : null;

    const titleRow = (
        <Title data-testid="title" $size={size}>
            {title}
            {!!percent && (
                <Pill
                    label={`${Math.abs(percent)}%`}
                    size="sm"
                    color={percent < 0 ? 'red' : 'green'}
                    leftIcon={percent < 0 ? TrendDown : TrendUp}
                    clickable={false}
                />
            )}
            {!!pillLabel && <Pill label={pillLabel} size="sm" color="primary" clickable={false} />}
            {pill !== null && pill !== undefined && pill}
        </Title>
    );

    const bodyContent = (
        <>
            {collapsible && subtitleBlock}
            {children}
        </>
    );

    return (
        <>
            {isEmpty ? (
                <CardContainer maxWidth={maxWidth} height={height} width={width} $size={size} data-testid={dataTestId}>
                    <TitleContainer data-testid="no-data">
                        <Title $isEmpty={isEmpty} $size={size}>
                            {t('noData')}
                        </Title>
                        <SubTitle $size={size}>{subTitle}</SubTitle>
                    </TitleContainer>
                </CardContainer>
            ) : (
                <CardContainer
                    isClickable={(!!button || onClick) && isCardClickable && !collapsible}
                    onClick={collapsible ? undefined : onClick}
                    maxWidth={maxWidth}
                    height={height}
                    width={width}
                    $size={size}
                    style={style}
                    data-testid={dataTestId}
                >
                    {title && (
                        <Header
                            iconAlignment={iconAlignment}
                            $size={size}
                            $collapsible={collapsible}
                            onClick={handleHeaderClick}
                        >
                            {icon && <div style={iconStyles}>{icon}</div>}

                            <TitleContainer>
                                {titleRow}
                                {!collapsible && subtitleBlock}
                            </TitleContainer>

                            {button}
                            {collapsible && (
                                <ExpandButton
                                    type="button"
                                    aria-label={isExpanded ? tc('collapse') : tc('expand')}
                                    aria-expanded={isExpanded}
                                    onClick={handleExpandToggle}
                                    data-testid="card-expand-button"
                                >
                                    <Icon icon={isExpanded ? CaretDown : CaretRight} size="md" color="inherit" />
                                </ExpandButton>
                            )}
                        </Header>
                    )}
                    {showBody &&
                        (collapsible ? (
                            <CollapsibleBody $size={size} data-testid="card-collapsible-body">
                                {bodyContent}
                            </CollapsibleBody>
                        ) : (
                            children
                        ))}
                </CardContainer>
            )}
        </>
    );
};
