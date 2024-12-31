import { Button, Card, PageTitle, Text } from '@src/alchemy-components';
import { capitalizeFirstLetter, pluralize } from '@src/app/shared/textUtil';
import { Maybe, UserUsageCounts } from '@src/types.generated';
import { countFormatter } from '@src/utils/formatter';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SectionKeys } from './utils';

const StatsContainer = styled.div`
    display: flex;
    padding: 12px 0;
    align-items: stretch;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const StatCards = styled.div`
    display: flex;
    gap: 20px;
`;

const VerticalDivider = styled(Divider)`
    height: auto;
    margin: 0 20px;
`;

const CARD_WIDTH = '225px';

interface Props {
    rowCount?: number;
    columnCount?: number;
    queryCount?: number;
    users?: Array<Maybe<UserUsageCounts>>;
    totalOperations?: number;
    scrollToSection?: (sectionKey: SectionKeys) => void;
    hasColumnStats?: boolean;
}

const StatsHighlights = ({
    rowCount,
    columnCount,
    queryCount,
    users,
    totalOperations,
    scrollToSection,
    hasColumnStats,
}: Props) => {
    const ViewButton = () => {
        return (
            <Button variant="text" icon="ArrowDownward">
                View
            </Button>
        );
    };
    return (
        <>
            <PageTitle
                title="Highlights"
                subTitle="View the latest statistics for this table"
                variant="sectionHeader"
            />
            <StatsContainer>
                <Section>
                    <Text size="sm" weight="bold">
                        Latest
                    </Text>
                    <StatCards>
                        <Card
                            title={countFormatter(rowCount || 0)}
                            subTitle={pluralize(rowCount || 0, 'Row')}
                            width={CARD_WIDTH}
                            isEmpty={rowCount === undefined}
                            button={rowCount ? <ViewButton /> : undefined}
                            onClick={() => (rowCount ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined)}
                        />
                        <Card
                            title={columnCount?.toString() || ''}
                            subTitle={pluralize(columnCount || 0, 'Column')}
                            width={CARD_WIDTH}
                            isEmpty={columnCount === undefined}
                            button={hasColumnStats ? <ViewButton /> : undefined}
                            onClick={() => (hasColumnStats ? scrollToSection?.(SectionKeys.COLUMN_STATS) : undefined)}
                        />
                    </StatCards>
                </Section>
                <VerticalDivider type="vertical" />
                <Section>
                    <Text size="sm" weight="bold">
                        Last 30 days
                    </Text>
                    <StatCards>
                        <Card
                            title={users?.length?.toString() || ''}
                            subTitle={pluralize(users?.length || 0, 'User')}
                            width={CARD_WIDTH}
                            isEmpty={users === undefined}
                            button={users && users.length ? <ViewButton /> : undefined}
                            onClick={() =>
                                users && users.length ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined
                            }
                        />
                        <Card
                            title={queryCount?.toString() || ''}
                            subTitle={capitalizeFirstLetter(pluralize(queryCount || 0, 'Query'))}
                            width={CARD_WIDTH}
                            isEmpty={queryCount === undefined}
                            button={queryCount ? <ViewButton /> : undefined}
                            onClick={() => (queryCount ? scrollToSection?.(SectionKeys.QUERIES) : undefined)}
                        />
                        <Card
                            title={totalOperations?.toString() || ''}
                            subTitle={pluralize(totalOperations || 0, 'Change')}
                            width={CARD_WIDTH}
                            isEmpty={totalOperations === undefined}
                            button={totalOperations ? <ViewButton /> : undefined}
                            onClick={() => (totalOperations ? scrollToSection?.(SectionKeys.CHANGES) : undefined)}
                        />
                    </StatCards>
                </Section>
            </StatsContainer>
        </>
    );
};

export default StatsHighlights;
