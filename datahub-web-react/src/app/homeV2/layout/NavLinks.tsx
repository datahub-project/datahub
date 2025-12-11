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

import { NavLinksMenu } from '@app/homeV2/layout/NavLinksMenu';

const Container = styled.div`
    border-radius: 47px;
    background-color: #7262d9;
    box-shadow: 0px 8px 8px 4px rgba(0, 0, 0, 0.25);
`;

export const NavLinks = () => {
    return (
        <Container>
            <NavLinksMenu />
        </Container>
    );
};
