import { Icon, colors } from '@components';
import { Skeleton } from 'antd';
import React, { HTMLAttributes } from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { LINEAGE_HANDLE_OFFSET } from '@app/lineageV3/common';
import MultiLineSkeleton from '@app/lineageV3/components/MultiLineSkeleton';
import { GenericPropertiesContextPath } from '@app/previewV2/ContextPath';
import OverflowTitle from '@app/sharedV2/text/OverflowTitle';

import { EntityType } from '@types';

const UnexpandedCardWrapper = styled.div`
    border-radius: 12px;

    flex-shrink: 0;
    width: 100%;
    overflow: hidden;

    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const SideElementsWrapper = styled.div`
    position: absolute;
    top: ${LINEAGE_HANDLE_OFFSET}px;
    width: 100%;
    left: 0;
    z-index: -2;
`;

const VerticalElementsWrapper = styled.div`
    position: absolute;
    left: 50%;
    top: 0;
    height: 100%;
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
    justify-content: center;
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
    urn: string;
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
    sideElements?: React.ReactNode;
    verticalElements?: React.ReactNode;

    childrenOpen: boolean;
    toggleChildren?: (e: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
    childrenText?: React.ReactNode;
}

export default function LineageCard({
    urn,
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
    verticalElements,
    childrenOpen,
    toggleChildren,
    childrenText,
    ...props
}: Props) {
    return (
        <UnexpandedCardWrapper data-testid={`unexpanded-lineage-card-${urn}`}>
            {loading || !name ? (
                <CardSkeleton extraDetails={extraDetails} />
            ) : (
                <CardWrapper {...props}>
                    <PlatformIconsWrapper>
                        {platformIcons.map((icon) => (
                            <PlatformIcon key={icon} src={icon} />
                        ))}
                    </PlatformIconsWrapper>
                    <BodyWrapper>
                        <TopRowWrapper>
                            {!!properties && <StyledContextPath properties={properties} hideTypeIcons linksDisabled />}
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
            <SideElementsWrapper>{sideElements}</SideElementsWrapper>
            <VerticalElementsWrapper>{verticalElements}</VerticalElementsWrapper>
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
