import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { REDESIGN_COLORS } from '../../../constants';

const EmptyContentMessage = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.DARK_GREY};
    opacity: 0.5;
`;

type Props = {
    message: string;
};

const EmptySectionText = ({ message }: Props) => {
    return <EmptyContentMessage type="secondary">{message}.</EmptyContentMessage>;
};

export default EmptySectionText;
