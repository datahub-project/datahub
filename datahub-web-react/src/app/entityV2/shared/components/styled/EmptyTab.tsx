import { EmptyState, LoadedImage } from '@components';
import React from 'react';
import styled from 'styled-components';

import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';

import NoDocs from '@images/no-docs.svg';

// The image-less variant sits inline in summary sections, which supply their own spacing.
const Container = styled.div<{ $compact?: boolean }>`
    padding: ${({ $compact }) => ($compact ? '0' : '40px')};
`;

// Callers pass their own controls (e.g. a button that opens a modal), which EmptyState's action props can't hold.
const Actions = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 16px;
    margin-top: 16px;
`;

type Props = {
    tab: string;
    hideImage?: boolean;
    children?: React.ReactNode;
};

export const EmptyTab = ({ tab, hideImage, children }: Props) => {
    return (
        <Container data-testid="empty-tab-description" $compact={hideImage}>
            <EmptyState
                title={EMPTY_MESSAGES[tab]?.title ?? ''}
                description={EMPTY_MESSAGES[tab]?.description}
                image={hideImage ? undefined : <LoadedImage src={NoDocs} alt="" width="86px" />}
            />
            {children && <Actions>{children}</Actions>}
        </Container>
    );
};
