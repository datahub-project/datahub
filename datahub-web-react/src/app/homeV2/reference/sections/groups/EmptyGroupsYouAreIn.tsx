import React from 'react';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyGroupsYouAreIn = () => {
    return <Text>You are not part of any groups yet.</Text>;
};
