/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    showHeader = true,
    showEmptyMessageHeader = true,
    emptyMessage = 'No stats collected for this asset at the moment.',
    dataTestId,
}: GraphCardProps) {
    const [showInfoModal, setShowInfoModal] = useState<boolean>(false);

    const handleModalClose = () => {
        setShowInfoModal(false);
    };

    return (
        <CardContainer maxWidth={width} data-testid={dataTestId}>
            {showHeader && (
                <GraphCardHeader>
                    <PageTitle title={title} subTitle={subTitle} variant="sectionHeader" />
                    <ControlsContainer>{renderControls?.()}</ControlsContainer>
                </GraphCardHeader>
            )}

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
                                    {showEmptyMessageHeader && (
                                        <Text size="2xl" weight="bold" color="gray">
                                            No Data
                                        </Text>
                                    )}
                                    <Text color="gray">{emptyMessage}</Text>
                                    {moreInfoModalContent && (
                                        <LinkText color="primary" onClick={() => setShowInfoModal(true)}>
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
