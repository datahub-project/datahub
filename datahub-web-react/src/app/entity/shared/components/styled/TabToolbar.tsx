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

export default styled.div`
    display: flex;
    position: relative;
    z-index: 1;
    justify-content: space-between;
    height: 46px;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 7px 16px;
    box-shadow: 0px 2px 6px 0px #0000000d;
    flex: 0 0 auto;
`;
