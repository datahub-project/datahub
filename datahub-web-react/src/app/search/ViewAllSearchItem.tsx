import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation();
    return (
        <ViewAllContainer>
            <ExploreForEntity>
                <SearchOutlined />
                <ExploreForEntityText>
                    {t('search.viewAllResultsFor')} <Typography.Text strong>{searchText}</Typography.Text>
                </ExploreForEntityText>
            </ExploreForEntity>
            <ReturnKey keyboard disabled>
                {t('common.return')}
            </ReturnKey>
        </ViewAllContainer>
    );
}

export default ViewAllSearchItem;
