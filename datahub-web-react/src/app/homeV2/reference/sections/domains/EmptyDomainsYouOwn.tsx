import React from 'react';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyDomainsYouOwn = () => {
    return (
        <Text>
            You are not the owner of any domains yet.
            <br />
            <a target="_blank" rel="noreferrer noopener" href="https://docs.datahub.com/docs/domains">
                Learn more
            </a>{' '}
            about domains.
        </Text>
    );
};
