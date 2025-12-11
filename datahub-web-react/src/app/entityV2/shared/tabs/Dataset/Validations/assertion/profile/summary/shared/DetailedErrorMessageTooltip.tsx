/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { InfoCircleOutlined } from '@ant-design/icons';
import { Popover } from '@components';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { DetailedErrorMessage } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/DetailedErrorMessage';

import { AssertionRunEvent } from '@types';

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    run: AssertionRunEvent;
    placement?: TooltipPlacement;
};

export const DetailedErrorMessageTooltip = ({ run, placement = 'bottom' }: Props) => {
    return (
        <Popover
            overlayStyle={{ maxWidth: 500 }}
            showArrow={false}
            placement={placement}
            content={<DetailedErrorMessage run={run} />}
        >
            <StyledInfoCircleOutlined />
        </Popover>
    );
};
