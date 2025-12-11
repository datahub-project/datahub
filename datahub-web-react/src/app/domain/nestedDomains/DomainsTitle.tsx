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

import DomainIcon from '@app/domain/DomainIcon';

const IconWrapper = styled.span`
    margin-right: 10px;
`;

export default function DomainsTitle() {
    return (
        <span>
            <IconWrapper>
                <DomainIcon />
            </IconWrapper>
            Domains
        </span>
    );
}
