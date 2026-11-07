import { Icon, Text } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
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

const ReturnKey = styled.span`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textDisabled};
    cursor: not-allowed;
    user-select: none;
`;

const Kbd = styled.kbd`
    margin: 0 0.2em;
    padding: 0.15em 0.4em 0.1em;
    font-size: 90%;
    background: ${(props) => props.theme.colors.bgSurface};
    border-radius: 3px;
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
                        components={{ strong: <Text type="span" size="inherit" weight="semiBold" /> }}
                    />
                </ExploreForEntityText>
            </ExploreForEntity>
            <ReturnKey>
                {/* eslint-disable-next-line i18next/no-literal-string */}
                <Kbd>⮐ return</Kbd>
            </ReturnKey>
        </ViewAllContainer>
    );
}

export default ViewAllSearchItem;
