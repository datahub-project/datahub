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

export const FilterContainer = styled.div<{ isCompact: boolean; isDisabled?: boolean }>`
    box-shadow: 0px 0px 4px 0px #00000010;
    border-radius: 10px;
    border: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.isCompact ? '0 4px' : '4px')};
    margin: ${(props) => (props.isCompact ? '2px 4px 2px 4px' : '4px')};

    ${(props) =>
        props.isDisabled
            ? `background: ${ANTD_GRAY[4]};`
            : `
            :hover {
                cursor: pointer;
                background: ${ANTD_GRAY[2]};
            }
    `}
`;
