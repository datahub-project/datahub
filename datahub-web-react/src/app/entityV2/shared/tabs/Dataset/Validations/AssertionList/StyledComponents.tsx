/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';

export const StyledTableContainer = styled.div`
    table tr.acryl-selected-table-row {
        background-color: ${ANTD_GRAY[4]};
    }
    margin: 0px 12px 12px 12px;
`;
