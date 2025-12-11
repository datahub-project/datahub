/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

export const AvatarStackContainer = styled.div`
    position: relative;
    display: flex;
    align-items: flex-start;
`;

export const AvatarContainer = styled.div`
    margin-left: -10px;
    &:first-child {
        margin-left: 0;
    }
`;
