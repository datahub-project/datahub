import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';


const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;
`;

type Props = {
    platforms: Array<string>;
};

const AutoCompletePlatformNames = ({ platforms }: Props) => {
    return <PlatformText>{platforms.join(' & ')}</PlatformText>;
};

export default AutoCompletePlatformNames;
