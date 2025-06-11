import { Icon, colors } from '@components';
import { Skeleton } from 'antd';
import React, { HTMLAttributes, useCallback } from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import MultiLineSkeleton from '@app/lineageV3/components/MultiLineSkeleton';
import { GenericPropertiesContextPath } from '@app/previewV2/ContextPath';
import OverflowTitle from '@app/sharedV2/text/OverflowTitle';

import { EntityType } from '@types';

const UnexpandedCardWrapper = styled.div`
    background-color: ${colors.white};
    border-radius: 12px;

    flex-shrink: 0;
    width: 100%;
    overflow: hidden;

    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

// Mimics height of primary card (without columns) for edge handles + actions
const SideElementsWrapper = styled.div<{ height?: number }>`
    position: absolute;
    height: ${({ height }) => height || 0}px;
    width: calc(100% - 1px); // Offset so arrows align with border
    left: -1px;
    z-index: -2;
`;

const CardWrapper = styled.div`
    display: flex;
    gap: 8px;
    padding: 8px;
`;

const PlatformIconsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 4px;
    min-width: 20px;
    padding: 4px 0;
`;

const BodyWrapper = styled.div`
    font-size: 14px;

    display: flex;
    flex-direction: column;
    min-width: 0;
    width: 100%;
`;

const TopRowWrapper = styled.div`
    font-size: 12px;

    max-height: 20px;

    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
`;

const MenuActionsWrapper = styled.span`
    font-size: 14px;

    display: flex;
    align-items: center;
    justify-content: end;
    gap: 8px;
`;

const ColumnButtonWrapper = styled.div`
    color: ${colors.gray[600]};
    font-weight: 600;
    letter-spacing: -0.06px;
    line-height: 1.5;
    cursor: pointer;

    border-top: 1px solid ${colors.gray[100]};
    border-bottom-left-radius: 12px;
    border-bottom-right-radius: 12px;

    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 0 8px;
    flex: 1 1 35px;
    min-height: 27px;
    max-height: 35px;

    :hover {
        background-color: ${colors.gray[1500]};
    }

    // TODO: Fix

    svg {
        color: ${colors.gray[1800]};
    }
`;

const PlatformIcon = styled.img`
    height: 16px;
    width: 16px;
`;

const PlatformIconSkeleton = styled(Skeleton.Avatar)`
    line-height: 0;
`;

const Title = styled(OverflowTitle)`
    color: ${colors.gray[600]};
    font-weight: 500;
`;

const StyledContextPath = styled(GenericPropertiesContextPath)`
    font-weight: 400;
`;

interface Props extends HTMLAttributes<HTMLDivElement> {
    type: EntityType;
    loading: boolean;

    // For rendering context path only
    properties?: GenericEntityProperties;

    name: string;
    nameExtra?: React.ReactNode;
    nameHighlight?: { text: string; color: string };
    platformIcons: string[];
    menuActions?: React.ReactNode[];

    // Section beneath name
    extraDetails?: React.ReactNode;

    // For rendering upstream / downstream elements like handles and expand/collapse buttons
    // Allows centering these elements on the main card (not including columns selector)
    sideElements?: React.ReactNode;

    childrenOpen: boolean;
    toggleChildren?: (e: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
    childrenText?: React.ReactNode;

    // Used to center the node around the edge handles
    baseNodeHeight?: number;
    setBaseNodeHeight?: (height: number) => void;
}

export default function LineageCard({
    type,
    loading,
    properties,
    name,
    nameExtra,
    nameHighlight,
    platformIcons,
    menuActions,
    extraDetails,
    sideElements,
    childrenOpen,
    toggleChildren,
    childrenText,
    baseNodeHeight,
    setBaseNodeHeight,
    ...props
}: Props) {
    const ref = useCallback(
        (node: HTMLDivElement | null) => {
            if (node !== null) {
                // No resize observer: make sure height on initial render is stable
                setBaseNodeHeight?.(node.scrollHeight);
            }
        },
        [setBaseNodeHeight],
    );

    return (
        <UnexpandedCardWrapper>
            {loading || !name ? (
                <CardSkeleton extraDetails={extraDetails} />
            ) : (
                <CardWrapper {...props} ref={ref}>
                    <PlatformIconsWrapper>
                        {platformIcons.map((icon) => (
                            <PlatformIcon key={icon} src={icon} />
                        ))}
                    </PlatformIconsWrapper>
                    <BodyWrapper>
                        <TopRowWrapper>
                            {!!properties && (
                                <StyledContextPath properties={properties} numVisible={2} hideTypeIcons linksDisabled />
                            )}
                            <MenuActionsWrapper>
                                {/* eslint-disable-next-line react/no-array-index-key */}
                                {menuActions?.map((action, index) => action && <span key={index}>{action}</span>)}
                            </MenuActionsWrapper>
                        </TopRowWrapper>
                        <Title
                            title={name}
                            highlightText={nameHighlight?.text}
                            highlightColor={nameHighlight?.color}
                            extra={nameExtra}
                        />
                        {extraDetails}
                    </BodyWrapper>
                </CardWrapper>
            )}
            <SideElementsWrapper height={baseNodeHeight}>{sideElements}</SideElementsWrapper>
            {loading
                ? (type === EntityType.Dataset || type === EntityType.Chart) && <ColumnDropdownSkeleton />
                : !!childrenText && (
                      <ColumnButtonWrapper data-testid="expand-contract-columns" onClick={toggleChildren}>
                          {childrenText}
                          {childrenOpen ? (
                              <Icon icon="CaretUp" source="phosphor" size="lg" />
                          ) : (
                              <Icon icon="CaretDown" source="phosphor" size="lg" />
                          )}
                      </ColumnButtonWrapper>
                  )}
        </UnexpandedCardWrapper>
    );
}

function CardSkeleton({ extraDetails }: Pick<Props, 'extraDetails'>) {
    return (
        <CardWrapper>
            <PlatformIconsWrapper>
                <PlatformIconSkeleton active size={16} />
            </PlatformIconsWrapper>
            <BodyWrapper>
                <MultiLineSkeleton numRows={extraDetails ? 3 : 2} />
            </BodyWrapper>
        </CardWrapper>
    );
}

function ColumnDropdownSkeleton() {
    return (
        <ColumnButtonWrapper>
            <MultiLineSkeleton numRows={1} />
        </ColumnButtonWrapper>
    );
}
