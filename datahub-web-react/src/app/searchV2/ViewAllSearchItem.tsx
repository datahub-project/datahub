import { Icon } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('search');
    return (
        <ViewAllContainer>
            <ExploreForEntity>
                <Icon icon={MagnifyingGlass} />
                <ExploreForEntityText>
                    <Trans
                        t={t}
                        i18nKey="viewAllResultsFor"
                        values={{ searchText }}
                        components={{ strong: <Typography.Text strong /> }}
                    />
                </ExploreForEntityText>
            </ExploreForEntity>
            {/* eslint-disable-next-line i18next/no-literal-string */}
            <ReturnKey keyboard disabled>
                ⮐ return
            </ReturnKey>
        </ViewAllContainer>
    );
}

export default ViewAllSearchItem;
