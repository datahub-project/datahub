import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const Text = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

export const EmptyGlossaryNodesYouOwn = () => {
    return (
        <Text>
            You have not created any glossary terms or groups yet. <br />
            <a
                target="_blank"
                rel="noreferrer noopener"
                href="https://datahubproject.io/docs/glossary/business-glossary/"
            >
                Learn more
            </a>{' '}
            about glossary items.
        </Text>
    );
};
