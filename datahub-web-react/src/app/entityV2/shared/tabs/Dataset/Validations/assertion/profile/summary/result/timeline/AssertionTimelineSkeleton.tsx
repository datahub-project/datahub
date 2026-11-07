import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const SKELETON_MARGIN_BOTTOM_PX = 12;
const NUM_GRID_BOXES = 12;
const GRID_BOX_WIDTH_RATIO = 1 / 4;
const GRID_BOX_WIDTH_PERCENT = GRID_BOX_WIDTH_RATIO * 100;
const NUM_GRID_ROWS = NUM_GRID_BOXES * GRID_BOX_WIDTH_RATIO;

const Body = styled.div`
    padding-bottom: ${SKELETON_MARGIN_BOTTOM_PX}px;
`;

const GridContainer = styled.div`
    display: flex;
`;

const Container = styled.div`
    padding: 0;
    width: 100%;
    height: 100%;
    position: relative;
`;

const GridRow = styled.div`
    display: flex;
    flex-flow: row wrap;
`;

const GridCol = styled.div`
    flex: 0 0 ${GRID_BOX_WIDTH_PERCENT}%;
    max-width: ${GRID_BOX_WIDTH_PERCENT}%;
    height: 100%;
    padding: 0;
    margin: 0;
    display: flex;
    justify-content: center;
`;

const Box = styled.div`
    width: 100%;
    height: 100%;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border: 1px solid ${(props) => props.theme.colors.border};
    display: flex;
    align-items: center;
    justify-content: center;
    margin: -1px 0 0 -1px;
`;

const GridSkeletonContainer = styled.div`
    width: 100%;
    height: 100%;
`;

const LoadingTextContainer = styled.div`
    display: flex;
    position: absolute;
    z-index: 10;
    width: 100%;
    height: 100%;
    justify-content: center;
    align-items: center;
`;
const LoadingText = styled(Text).attrs({ type: 'span' })`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textDisabled};
    line-height: inherit;
    word-break: break-word;
`;

const renderGridSkeleton = (height: number, loadingText: string) => {
    const boxes = Array.from({ length: NUM_GRID_BOXES }, (_, index) => (
        <GridCol key={index}>
            <Box style={{ height: height / NUM_GRID_ROWS }} />
        </GridCol>
    ));

    return (
        <Container>
            <LoadingTextContainer>
                <LoadingText>{loadingText}</LoadingText>
            </LoadingTextContainer>
            <GridRow>{boxes}</GridRow>
        </Container>
    );
};

type Props = {
    parentDimensions: { width: number; height: number };
};

export const AssertionTimelineSkeleton = (props: Props) => {
    const { t: tc } = useTranslation('common.feedback');
    const skeletonHeight = props.parentDimensions.height - SKELETON_MARGIN_BOTTOM_PX;
    return (
        <Body style={props.parentDimensions}>
            <GridContainer style={{ height: skeletonHeight }}>
                <GridSkeletonContainer>{renderGridSkeleton(skeletonHeight, tc('loading'))}</GridSkeletonContainer>
            </GridContainer>
        </Body>
    );
};
