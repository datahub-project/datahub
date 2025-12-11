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

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const SecondaryText = styled.div`
    color: ${ANTD_GRAY[7]};
`;

/**
 * No results yet summarization.
 */
export const NoResultsSummary = () => {
    return <SecondaryText>This assertion has not been evaluated yet! Come back later to view results.</SecondaryText>;
};
