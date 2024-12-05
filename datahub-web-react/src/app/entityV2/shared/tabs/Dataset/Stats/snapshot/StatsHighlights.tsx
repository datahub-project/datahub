import { Button, Card, PageTitle, Text } from '@src/alchemy-components';
import { capitalizeFirstLetter, pluralize } from '@src/app/shared/textUtil';
import { Maybe, UserUsageCounts } from '@src/types.generated';
import { countFormatter } from '@src/utils/formatter';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

interface Props {
    rowCount?: number;
    columnCount?: number;
    queryCount?: number;
    users?: Array<Maybe<UserUsageCounts>>;
}

const HighlightsSection = styled.div`
    padding: 16px 24px;
`;

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

const StatsHighlights = ({ rowCount, columnCount, queryCount, users }: Props) => {
    const ViewButton = () => {
        return (
            <Button variant="text" icon="ArrowDownward">
                View
            </Button>
        );
    };
    return (
        <HighlightsSection>
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
                        {rowCount && <Card title={countFormatter(rowCount)} subTitle="Rows" width={CARD_WIDTH} />}
                        {columnCount && (
                            <Card
                                title={columnCount.toString()}
                                subTitle={pluralize(columnCount, 'Column')}
                                button={<ViewButton />}
                                width={CARD_WIDTH}
                            />
                        )}
                    </StatCards>
                </Section>
                <VerticalDivider type="vertical" />
                <Section>
                    <Text size="sm" weight="bold">
                        Last 30 days
                    </Text>
                    <StatCards>
                        {users && users.length > 0 && (
                            <Card
                                title={users.length.toString()}
                                subTitle={pluralize(users.length, 'User')}
                                width={CARD_WIDTH}
                            />
                        )}
                        {queryCount && (
                            <Card
                                title={queryCount?.toString()}
                                subTitle={capitalizeFirstLetter(pluralize(queryCount, 'Query'))}
                                button={<ViewButton />}
                                width={CARD_WIDTH}
                            />
                        )}
                    </StatCards>
                </Section>
            </StatsContainer>
        </HighlightsSection>
    );
};

export default StatsHighlights;
