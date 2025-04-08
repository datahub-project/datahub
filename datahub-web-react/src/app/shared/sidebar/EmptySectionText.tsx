import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const EmptyContentMessage = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
    color: #56668e;
    opacity: 0.5;
`;

type Props = {
    message: string;
};

const EmptySectionText = ({ message }: Props) => {
    return <EmptyContentMessage type="secondary">{message}.</EmptyContentMessage>;
};

export default EmptySectionText;
