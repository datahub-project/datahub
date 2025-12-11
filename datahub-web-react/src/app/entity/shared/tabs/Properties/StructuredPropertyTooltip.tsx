/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { StructuredPropertyEntity } from '@types';

const ContentWrapper = styled.div`
    font-size: 12px;
`;

const Header = styled.div`
    font-size: 10px;
`;

const Description = styled.div`
    padding-left: 16px;
`;

interface Props {
    structuredProperty: StructuredPropertyEntity;
}

export default function StructuredPropertyTooltip({ structuredProperty }: Props) {
    return (
        <ContentWrapper>
            <Header>Structured Property</Header>
            <div>{structuredProperty.definition.displayName || structuredProperty.definition.qualifiedName}</div>
            {structuredProperty.definition.description && (
                <Description>{structuredProperty.definition.description}</Description>
            )}
        </ContentWrapper>
    );
}
