import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const Text = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

export const EmptyGroupsYouAreIn = () => {
    return <Text>You are not part of any groups yet.</Text>;
};
