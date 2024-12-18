import React from 'react';
import { Skeleton } from 'antd';
import styled from 'styled-components';
import { CardContainer } from '../Card/components';
import { PageTitle } from '../PageTitle';
import { Text } from '../Text';
import { ControlsContainer, EmptyMessageContainer, GraphCardBody, GraphCardHeader, GraphContainer } from './components';
import { GraphCardProps } from './types';

const EmptyMessageWrapper = styled.div`
    text-align: center;
`;

export function GraphCard({
    title,
    subTitle,
    loading,
    graphHeight = '350px',
    width = '100%',
    renderGraph,
    renderControls,
    isEmpty,
    emptyContent,
}: GraphCardProps) {
    return (
        <CardContainer width={width}>
            <GraphCardHeader>
                <PageTitle title={title} subTitle={subTitle} variant="sectionHeader" />
                <ControlsContainer>{renderControls?.()}</ControlsContainer>
            </GraphCardHeader>

            {loading && <Skeleton.Button style={{ width: '100%', height: graphHeight }} active />}

            {!loading && (
                <GraphCardBody>
                    <GraphContainer $height={graphHeight} $isEmpty={isEmpty}>
                        {renderGraph()}
                    </GraphContainer>
                    {isEmpty &&
                        (emptyContent || (
                            <EmptyMessageContainer>
                                <EmptyMessageWrapper>
                                    <Text size="2xl" weight="bold" color="gray">
                                        No Data
                                    </Text>
                                    <Text color="gray">No stats collected for this asset at the moment.</Text>
                                    <Text>
                                        <a href="/">More info</a>
                                    </Text>
                                </EmptyMessageWrapper>
                            </EmptyMessageContainer>
                        ))}
                </GraphCardBody>
            )}
        </CardContainer>
    );
}
