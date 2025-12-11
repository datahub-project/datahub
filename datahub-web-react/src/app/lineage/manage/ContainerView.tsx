/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
