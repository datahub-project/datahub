import React from 'react';

import { StyledRightOutlined } from '@app/entity/shared/containers/profile/header/PlatformContent/ParentNodesView';

import { Container } from '@types';

export interface ContainerViewProps {
    directContainer: Container | undefined | null;
    remainingContainers: Container[] | undefined | null;
}

export function ContainerView({ directContainer, remainingContainers }: ContainerViewProps) {
    return (
        <>
            {remainingContainers &&
                remainingContainers.map((c) => (
                    <>
                        <StyledRightOutlined data-testid="right-arrow" />
                        <span>{c?.properties?.name}</span>
                    </>
                ))}
            {directContainer && (
                <>
                    <StyledRightOutlined data-testid="right-arrow" />
                    <span>{directContainer?.properties?.name}</span>
                </>
            )}
        </>
    );
}
