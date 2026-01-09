import Icon from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { Text, colors } from '@src/alchemy-components';

import NoStatsAvailble from '@images/no-stats-available.svg?react';

const NoDataContainer = styled.div`
    margin: 40px auto;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const StyledIcon = styled(Icon)`
    font-size: 80px;
    margin-bottom: 6px;
    color: ${colors.white};
`;

export default function NoStats() {
    return (
        <NoDataContainer>
            <StyledIcon component={NoStatsAvailble} />
            <Text color="gray" size="sm">
                No column statistics found
            </Text>
        </NoDataContainer>
    );
}
