/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

export const SuggestionText = styled.div`
    margin-left: 12px;
    margin-top: 2px;
    margin-bottom: 2px;
    color: ${ANTD_GRAY[9]};
    font-size: 16px;
    overflow: hidden;
`;
