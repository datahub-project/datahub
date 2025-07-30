import React, { useState } from 'react';
import styled from 'styled-components';

import { CardContainer } from '@components/components/Card/components';
import MoreInfoModal from '@components/components/GraphCard/MoreInfoModal';
import {
    ControlsContainer,
    EmptyMessageContainer,
    GraphCardBody,
    GraphCardHeader,
    GraphContainer,
    LoaderContainer,
} from '@components/components/GraphCard/components';
import { GraphCardProps } from '@components/components/GraphCard/types';
import { Loader } from '@components/components/Loader';
import { PageTitle } from '@components/components/PageTitle';
import { Text } from '@components/components/Text';

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
