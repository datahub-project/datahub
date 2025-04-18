import { ThunderboltFilled } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const Logo = styled(ThunderboltFilled)`
    color: ${REDESIGN_COLORS.BLUE};
`;

type Props = {
    className?: string;
};

export const InferredAssertionLogo = ({ className }: Props) => {
    return <Logo className={className} />;
};
