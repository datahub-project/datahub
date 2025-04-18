import React from 'react';
import styled from 'styled-components';
import { ThunderboltFilled } from '@ant-design/icons';
import { REDESIGN_COLORS } from '../../../constants';

const Logo = styled(ThunderboltFilled)`
    color: ${REDESIGN_COLORS.BLUE};
`;

type Props = {
    className?: string;
};

export const InferredAssertionLogo = ({ className }: Props) => {
    return <Logo className={className} />;
};
