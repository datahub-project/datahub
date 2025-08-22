import { Button, Text, colors } from '@components';
import { Megaphone, X } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

const BannerContainer = styled.div`
    display: flex;
    align-items: center;
    background: ${colors.primary[0]};
    border-radius: 8px;
    padding: 4px 12px;
    margin-bottom: 16px;
    gap: 12px;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    color: ${colors.primary[500]};
`;

const TextWrapper = styled.div`
    flex: 1;
    color: ${colors.primary[500]};
`;

const CloseButton = styled(Button)``;

const STORAGE_KEY = 'datahub-tuning-help-banner-dismissed';

export const TuningHelpBanner = () => {
    const [isVisible, setIsVisible] = useState(false);

    useEffect(() => {
        // Check if banner has been dismissed before
        const isDismissed = localStorage.getItem(STORAGE_KEY);
        if (!isDismissed) {
            setIsVisible(true);
        }
    }, []);

    const handleClose = () => {
        setIsVisible(false);
        localStorage.setItem(STORAGE_KEY, 'true');
    };

    if (!isVisible) {
        return null;
    }

    return (
        <BannerContainer>
            <IconWrapper>
                <Megaphone size={20} weight="fill" />
            </IconWrapper>
            <TextWrapper>
                <Text size="md" weight="medium">
                    Drag your cursor around bad training data to exclude it and improve model quality
                </Text>
            </TextWrapper>
            <CloseButton onClick={handleClose} aria-label="Close banner" variant="text" size="xs">
                <X size={16} weight="bold" />
            </CloseButton>
        </BannerContainer>
    );
};
