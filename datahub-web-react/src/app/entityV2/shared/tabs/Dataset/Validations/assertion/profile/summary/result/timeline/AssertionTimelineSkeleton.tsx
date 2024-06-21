import React from 'react';
import { Col, Row, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../../../../../constants';

const SKELETON_MARGIN_BOTTOM_PX = 12;
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

const StyledCol = styled(Col)`
    height: 100%;
    padding: 0;
    margin: 0;
    display: flex;
    justify-content: center;
`;

const Box = styled.div`
    width: 100%;
    height: 100%;
    background-color: ${ANTD_GRAY[3]};
    border: 1px solid ${ANTD_GRAY[4]};
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
const LoadingText = styled(Typography.Text)`
    font-size: 14px;
    color: ${ANTD_GRAY[6]};
`;

const NUM_GRID_BOXES = 12;
const ANT_COL_6_SPAN = 6;
const ANT_COL_6_ITEM_WIDTH_RATIO = 1 / 4;
const NUM_GRID_ROWS = NUM_GRID_BOXES * ANT_COL_6_ITEM_WIDTH_RATIO;
const renderGridSkeleton = (height: number) => {
    const boxes = Array.from({ length: NUM_GRID_BOXES }, (_, index) => (
        <StyledCol key={index} span={ANT_COL_6_SPAN}>
            <Box style={{ height: height / NUM_GRID_ROWS }} />
        </StyledCol>
    ));

    return (
        <Container>
            <LoadingTextContainer>
                <LoadingText>Loading...</LoadingText>
            </LoadingTextContainer>
            <Row gutter={0}>{boxes}</Row>
        </Container>
    );
};

type Props = {
    parentDimensions: { width: number; height: number };
};

export const AssertionTimelineSkeleton = (props: Props) => {
    const skeletonHeight = props.parentDimensions.height - SKELETON_MARGIN_BOTTOM_PX;
    return (
        <Body style={props.parentDimensions}>
            <GridContainer style={{ height: skeletonHeight }}>
                <GridSkeletonContainer>{renderGridSkeleton(skeletonHeight)}</GridSkeletonContainer>
            </GridContainer>
        </Body>
    );
};
