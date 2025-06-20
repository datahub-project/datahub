import { Icon } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

const ExploreForEntity = styled.span`
    font-weight: light;
    font-size: 16px;
    padding: 5px 0;
`;

const ExploreForEntityText = styled.span`
    margin-left: 10px;
`;

const ViewAllContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
`;

const ReturnKey = styled(Typography.Text)`
    & kbd {
        border: none;
    }
    font-size: 12px;
`;

function ViewAllSearchItem({ searchTarget: searchText }: { searchTarget?: string }) {
    return (
        <ViewAllContainer>
            <ExploreForEntity>
                <Icon icon="MagnifyingGlass" source="phosphor" />
                <ExploreForEntityText>
                    View all results for <Typography.Text strong>{searchText}</Typography.Text>
                </ExploreForEntityText>
            </ExploreForEntity>
            <ReturnKey keyboard disabled>
                ⮐ return
            </ReturnKey>
        </ViewAllContainer>
    );
}

export default ViewAllSearchItem;
