import React from 'react';
import styled from 'styled-components';
import { CardContainer } from '../Card/components';
import { Loader } from '../Loader';
import { PageTitle } from '../PageTitle';
import { Text } from '../Text';
import {
    ControlsContainer,
    EmptyMessageContainer,
    GraphCardBody,
    GraphCardHeader,
    GraphContainer,
    LoaderContainer,
} from './components';
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
        <CardContainer maxWidth={width}>
            <GraphCardHeader>
                <PageTitle title={title} subTitle={subTitle} variant="sectionHeader" />
                <ControlsContainer>{renderControls?.()}</ControlsContainer>
            </GraphCardHeader>

            {loading && (
                <LoaderContainer $height={graphHeight}>
                    <Loader />
                </LoaderContainer>
            )}

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
                                </EmptyMessageWrapper>
                            </EmptyMessageContainer>
                        ))}
                </GraphCardBody>
            )}
        </CardContainer>
    );
}
