import React from 'react';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyGlossaryNodesYouOwn = () => {
    return (
        <Text>
            You have not created any glossary terms or groups yet. <br />
            <a
                target="_blank"
                rel="noreferrer noopener"
                href="https://docs.datahub.com/docs/glossary/business-glossary/"
            >
                Learn more
            </a>{' '}
            about glossary items.
        </Text>
    );
};
