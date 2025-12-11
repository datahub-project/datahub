/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Icon, Text } from '@components';
import React from 'react';
import { FallbackProps } from 'react-error-boundary';
import styled from 'styled-components';

import SmallModule from '@app/homeV3/module/components/SmallModule';
import { ModuleProps } from '@app/homeV3/module/types';

const Container = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledText = styled(Text)`
    text-overflow: ellipsis;
    white-space: nowrap;
    overflow: hidden;
`;

const StyledIcon = styled(Icon)`
    flex-shrink: 0;
`;

interface Props {
    fallbackProps: FallbackProps;
    moduleProps: ModuleProps;
}

export default function SmallModuleFallback({ moduleProps, fallbackProps }: Props) {
    return (
        <SmallModule {...moduleProps}>
            <Container>
                <StyledIcon icon="ExclamationMark" source="phosphor" color="gray" colorLevel={1800} size="4xl" />
                <StyledText weight="bold" size="lg" color="gray" colorLevel={600}>
                    Error loading. Try refreshing.
                </StyledText>
                <Button
                    icon={{ icon: 'ArrowsCounterClockwise', source: 'phosphor', size: '2xl' }}
                    variant="link"
                    onClick={() => fallbackProps.resetErrorBoundary()}
                />
            </Container>
        </SmallModule>
    );
}
