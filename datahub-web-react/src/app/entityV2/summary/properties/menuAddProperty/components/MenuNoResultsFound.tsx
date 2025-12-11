/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const NoResultsFoundWrapper = styled.div`
    text-align: center;
    padding: 4px;
`;

export default function MenuNoResultsFound() {
    return (
        <NoResultsFoundWrapper>
            <Text color="gray">No results found</Text>
        </NoResultsFoundWrapper>
    );
}
