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

import LinksList from '@app/entityV2/summary/links/LinksList';

const LinksSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export default function Links() {
    return (
        <LinksSection>
            <LinksList />
        </LinksSection>
    );
}
