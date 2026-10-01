import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import { Spin } from 'antd';
import React from 'react';
import styled, { keyframes } from 'styled-components';

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

const SpinIcon = styled(CircleNotch)`
    animation: ${spin} 1s linear infinite;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const SidebarEntitiesLoadingSection = () => {
    return <Spin indicator={<SpinIcon />} />;
};

export default SidebarEntitiesLoadingSection;
