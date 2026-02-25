import React from 'react';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyTagsYouOwn = () => {
    return (
        <Text>
            You have not created any tags yet.
            <br />
            <a target="_blank" rel="noreferrer noopener" href="https://docs.datahub.com/docs/tags">
                Learn more
            </a>{' '}
            about tags.
        </Text>
    );
};
