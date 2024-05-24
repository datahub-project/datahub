import React from 'react';
import { Col, Row, Skeleton } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../../../../../constants';

const Body = styled.div`
    margin-bottom: 10px;
`;

const GridContainer = styled.div`
    display: flex;
`;

const CustomRow = styled(({ direction: _direction, ...rest }) => <Row {...rest} />)`
    margin-bottom: ${({ direction }) => (direction === 'vertical' ? '16px' : '0')};
    margin-right: ${({ direction }) => (direction === 'horizontal' ? '16px' : '0')};
`;

const Container = styled.div`
    padding: 0;
    width: 400px;
`;

const StyledCol = styled(Col)`
    padding: 0;
    margin: 0;
    display: flex;
    justify-content: center;
`;

const StyledRow = styled(Row)`
    margin-left: 80px;
`;

const Box = styled.div`
    width: 100%;
    height: 60px;
    background-color: ${ANTD_GRAY[3]};
    border: 1px solid ${ANTD_GRAY[4]};
    display: flex;
    align-items: center;
    justify-content: center;
    margin: -1px 0 0 -1px;
`;

const ButtonContainer = styled.div`
    margin-right: 16px;
`;

const GridSkeletonContainer = styled.div``;

const renderSkeletonButtons = (count, direction) => {
    return Array.from({ length: count }, (_, index) => (
        <CustomRow key={index} direction={direction}>
            <Col span={24}>
                <Skeleton.Button active />
            </Col>
        </CustomRow>
    ));
};

const renderGridSkeleton = () => {
    const boxes = Array.from({ length: 12 }, (_, index) => (
        <StyledCol key={index} span={6}>
            <Box />
        </StyledCol>
    ));

    return (
        <Container>
            <Row gutter={0}>{boxes}</Row>
        </Container>
    );
};

export const AssertionTimelineSkeleton = () => {
    const gridSize = 4;

    return (
        <Body>
            <GridContainer>
                <ButtonContainer>{renderSkeletonButtons(gridSize, 'vertical')}</ButtonContainer>
                <GridSkeletonContainer>{renderGridSkeleton()}</GridSkeletonContainer>
            </GridContainer>

            <StyledRow>
                {Array.from({ length: 5 }, (_, index) => (
                    <Col span={4} key={index}>
                        <Skeleton.Button active />
                    </Col>
                ))}
            </StyledRow>
        </Body>
    );
};
