import React from 'react';

import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../../../constants';

const SecondaryText = styled.div`
    color: ${ANTD_GRAY[7]};
`;

/**
 * No results yet summarization.
 */
export const NoResultsSummary = () => {
    return <SecondaryText>This assertion has not been evaluated yet! Come back later to view results.</SecondaryText>;
};
