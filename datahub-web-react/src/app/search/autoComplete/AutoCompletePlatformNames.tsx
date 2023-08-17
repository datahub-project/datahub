import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 500;
    color: ${ANTD_GRAY_V2[8]};
    white-space: nowrap;
`;

type Props = {
    platforms: Array<string>;
};

const AutoCompletePlatformNames = ({ platforms }: Props) => {
    return <PlatformText>{platforms.join(' & ')}</PlatformText>;
};

export default AutoCompletePlatformNames;
