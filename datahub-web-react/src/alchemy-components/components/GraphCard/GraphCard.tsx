import React, { useState } from 'react';
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
    GraphCardHeaderRow,
    GraphContainer,
    LoaderContainer,
} from './components';
import { GraphCardProps } from './types';
import MoreInfoModal from './MoreInfoModal';
import { SubTitle } from '../PageTitle/components';

const EmptyMessageWrapper = styled.div`
    text-align: center;
`;

const LinkText = styled(Text)`
    display: inline;
    :hover {
        cursor: pointer;
    }
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
    moreInfoModalContent,
}: GraphCardProps) {
    const [showInfoModal, setShowInfoModal] = useState<boolean>(false);

    const handleModalClose = () => {
        setShowInfoModal(false);
    };

    return (
        <CardContainer maxWidth={width}>
            <GraphCardHeader>
                <GraphCardHeaderRow>
                    <PageTitle title={title} variant="sectionHeader" />
                    <ControlsContainer>{renderControls?.()}</ControlsContainer>
                </GraphCardHeaderRow>
                <GraphCardHeaderRow>
                    <SubTitle variant="sectionHeader">{subTitle}</SubTitle>
                </GraphCardHeaderRow>
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
                                    {moreInfoModalContent && (
                                        <LinkText color="violet" onClick={() => setShowInfoModal(true)}>
                                            More info
                                        </LinkText>
                                    )}
                                </EmptyMessageWrapper>
                                <MoreInfoModal
                                    showModal={showInfoModal}
                                    handleClose={handleModalClose}
                                    modalContent={moreInfoModalContent}
                                />
                            </EmptyMessageContainer>
                        ))}
                </GraphCardBody>
            )}
        </CardContainer>
    );
}
