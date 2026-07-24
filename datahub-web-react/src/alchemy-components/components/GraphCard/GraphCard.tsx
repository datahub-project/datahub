import { Button } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
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

const LinkButton = styled(Button)`
    display: inline;
    padding: 0;
    min-width: unset;
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
    emptyMessage = undefined,
    dataTestId,
}: GraphCardProps) {
    const { t } = useTranslation('alchemy');
    const [showInfoModal, setShowInfoModal] = useState<boolean>(false);
    const resolvedEmptyMessage = emptyMessage ?? t('graphCard.noStats');

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
                    <GraphContainer
                        $height={graphHeight}
                        $isEmpty={isEmpty}
                        data-testid={isEmpty ? `${dataTestId}-chart-empty` : `${dataTestId}-chart`}
                    >
                        {renderGraph()}
                    </GraphContainer>
                    {isEmpty &&
                        (emptyContent || (
                            <EmptyMessageContainer>
                                <EmptyMessageWrapper>
                                    {showEmptyMessageHeader && (
                                        <Text size="2xl" weight="bold">
                                            {t('noData')}
                                        </Text>
                                    )}
                                    <Text>{resolvedEmptyMessage}</Text>
                                    {moreInfoModalContent && (
                                        <LinkButton
                                            variant="text"
                                            color="primary"
                                            onClick={() => setShowInfoModal(true)}
                                        >
                                            {t('graphCard.moreInfo')}
                                        </LinkButton>
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
