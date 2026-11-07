import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const PLATFORM_JOIN_SEPARATOR = ' & ';

const PlatformText = styled(Text)`
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
    return <PlatformText type="span">{platforms.join(PLATFORM_JOIN_SEPARATOR)}</PlatformText>;
};

export default AutoCompletePlatformNames;
