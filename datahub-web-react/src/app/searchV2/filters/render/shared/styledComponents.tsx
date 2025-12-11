/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { MoreFilterOptionLabel } from '@app/searchV2/filters/styledComponents';

export const SearchFilterWrapper = styled.div`
    padding: 0 25px 15px 25px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

export const Title = styled.div`
    align-items: center;
    font-weight: bold;
    display: flex;
    justify-content: left;
    cursor: pointer;
`;

export const StyledMoreFilterOptionLabel = styled(MoreFilterOptionLabel)`
    justify-content: left;
`;
