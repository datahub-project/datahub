import { Tooltip, colors } from '@components';
import { X } from '@phosphor-icons/react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import {
    useDismissProductAnnouncement,
    useGetLatestProductAnnouncementData,
    useIsProductAnnouncementEnabled,
    useIsProductAnnouncementVisible,
} from '@app/shared/product/update/hooks';

const CardWrapper = styled.div`
    position: fixed;
    bottom: 24px;
    left: 16px;
    width: 240px;
    border-radius: 12px;
    box-shadow: 0px 0px 6px 0px #5d668b33;
    background: white;
    z-index: 1000;
    overflow: hidden;
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 12px 0px 12px;
`;

const Title = styled.h3`
    font-size: 14px;
    font-family: Mulish;
    font-weight: 700;
    color: ${colors.gray[600]};
`;

const CloseButton = styled.button`
    background: none;
    border: none;
    color: #9ca3af;
    cursor: pointer;

    &:hover {
        color: #4b5563;
    }
`;

const ImageSection = styled.div`
    margin: 0px 12px 12px 12px;
`;

const Image = styled.img`
    width: 100%;
    height: auto;
`;

const Content = styled.div`
    padding: 0px 12px 12px 12px;
`;

const Description = styled.p`
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

const CTA = styled.a`
    font-size: 14px;
    color: ${colors.primary[500]};
    text-decoration: none;
    font-weight: 500;

    &:hover {
        text-decoration: underline;
    }
`;

const StyledCloseRounded = styled(X)`
    font-size: 16px;
`;

export default function ProductUpdates() {
    const isFeatureEnabled = useIsProductAnnouncementEnabled();
    const latestUpdate = useGetLatestProductAnnouncementData();
    const { title, image, description, ctaText, ctaLink } = latestUpdate;

    const { visible, refetch } = useIsProductAnnouncementVisible(latestUpdate);
    const dismiss = useDismissProductAnnouncement(latestUpdate, refetch);

    // Local state to hide immediately on dismiss
    const [isLocallyVisible, setIsLocallyVisible] = useState(false);

    useEffect(() => {
        setIsLocallyVisible(visible);
    }, [visible]);

    const handleDismiss = () => {
        setIsLocallyVisible(false);
        dismiss();
    };

    const trackClick = () => {
        analytics.event({
            type: EventType.ClickProductUpdate,
            id: latestUpdate.id,
            url: latestUpdate.ctaLink,
        });
    };

    if (!isFeatureEnabled || !isLocallyVisible || !latestUpdate.enabled) return null;

    return (
        <CardWrapper>
            <Header>
                <Title>{title}</Title>
                <Tooltip title="Dismiss" placement="right">
                    <CloseButton onClick={handleDismiss}>
                        <StyledCloseRounded />
                    </CloseButton>
                </Tooltip>
            </Header>
            {image && (
                <ImageSection>
                    <Image src={image} alt="" />
                </ImageSection>
            )}
            <Content>
                {description && <Description>{description}</Description>}
                {ctaText && ctaLink && (
                    <CTA href={ctaLink} onClick={trackClick} target="_blank" rel="noreferrer noopener">
                        {ctaText} →
                    </CTA>
                )}
            </Content>
        </CardWrapper>
    );
}
