import React from 'react';
import styled from 'styled-components';
import { Empty, List } from 'antd';
import { Test } from '../../types.generated';
import { TestCard } from './card/TestCard';
import { TestsSectionTitle } from './TestsSectionTitle';

const Container = styled.div``;

type Props = {
    title: string;
    tooltip?: string;
    tests: Test[];
};

/**
 * Represents a single section of Metadata Tests.
 *
 * In the future, we may load an individual test section
 * from within this section itself, by providing the name / identifier for the section.
 *
 * For now, tests are injected via a top-level test provider responsible for fetching and updating Tests.
 */
export const TestsSection = ({ title, tooltip, tests }: Props) => {
    return (
        <Container>
            <TestsSectionTitle title={title} tooltip={tooltip} />
            <List
                dataSource={tests}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Tests found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                grid={{ gutter: 16, column: 4 }}
                pagination={false}
                renderItem={(test, index) => {
                    return (
                        <List.Item key={test.urn}>
                            <TestCard test={test} index={index} />
                        </List.Item>
                    );
                }}
            />
        </Container>
    );
};
