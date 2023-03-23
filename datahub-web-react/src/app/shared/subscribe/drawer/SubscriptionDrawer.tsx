import React from 'react';
import styled from 'styled-components/macro';
import { Button, Drawer, Typography } from 'antd';
import { CloseCircleOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import NotificationTypesSection from './section/NotificationTypesSection';
import UpstreamSection from './section/UpstreamSection';
import NotificationRecipientSection from './section/NotificationRecipientSection';
import Footer from './section/Footer';
import SelectGroupSection from './section/SelectGroupSection';

const SubscribeDrawer = styled(Drawer)``;

const SubscriptionTitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
`;

const SubscriptionTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 24px;
    line-height: 32px;
    font-weight: 400;
`;

const IngestionInfoContainer = styled.div`
    margin-top: 24px;
    margin-left: 8px;
    display: grid;
    align-items: baseline;
    column-gap: 10px;
`;

const IngestionInfoIcon = styled(InfoCircleOutlined)`
    grid-row: 1;
    grid-column: 1;
`;

const IngestionInfoText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[8]};
    grid-row: 1;
    grid-column: 2;
`;

interface Props {
    isOpen: boolean;
    onClose: () => void;
    isPersonal: boolean;
}

export default function SubscriptionDrawer({ isOpen, onClose, isPersonal }: Props) {
    const { entityData } = useEntityData();
    const entityName = entityData?.name || '';
    return (
        <>
            <SubscribeDrawer
                width={512}
                footer={<Footer onClose={onClose} />}
                open={isOpen}
                onClose={onClose}
                closable={false}
            >
                <SubscriptionTitleContainer>
                    <SubscriptionTitle>Subscribe to {entityName}</SubscriptionTitle>
                    <Button type="text" onClick={onClose}>
                        <CloseCircleOutlined style={{ color: ANTD_GRAY[10] }} />
                    </Button>
                </SubscriptionTitleContainer>
                {!isPersonal && <SelectGroupSection />}
                <NotificationTypesSection />
                <IngestionInfoContainer>
                    <IngestionInfoIcon />
                    <IngestionInfoText>
                        You will be notified as soon as DataHub is aware of changes based on ingestion frequency.
                    </IngestionInfoText>
                </IngestionInfoContainer>
                <UpstreamSection />
                <NotificationRecipientSection isPersonal={isPersonal} />
            </SubscribeDrawer>
        </>
    );
}
