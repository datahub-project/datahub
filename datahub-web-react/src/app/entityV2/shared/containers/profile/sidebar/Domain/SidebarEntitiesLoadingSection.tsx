import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import { Spin } from 'antd';
import React from 'react';
import styled from 'styled-components';

const SpinIcon = styled(CircleNotch)`
    color: ${(props) => props.theme.colors.textTertiary};
`;

const SidebarEntitiesLoadingSection = () => {
    return <Spin indicator={<SpinIcon />} />;
};

export default SidebarEntitiesLoadingSection;
