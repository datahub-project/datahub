import React from 'react';

import styled from 'styled-components';
import { Popover } from '@components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { TooltipPlacement } from 'antd/lib/tooltip';

import { ANTD_GRAY } from '../../../../../../../constants';
import { AssertionRunEvent } from '../../../../../../../../../../types.generated';
import { DetailedErrorMessage } from './DetailedErrorMessage';

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
