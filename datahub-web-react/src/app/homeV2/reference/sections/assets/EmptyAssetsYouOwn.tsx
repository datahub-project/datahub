import React from 'react';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyAssetsYouOwn = () => {
    return (
        <Text>
            You do not own any assets yet.
            <br />
            <a target="_blank" rel="noreferrer noopener" href="https://docs.datahub.com/docs/ownership/ownership-types">
                Learn more
            </a>{' '}
            about ownership.
        </Text>
    );
};
