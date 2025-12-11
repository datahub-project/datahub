/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
