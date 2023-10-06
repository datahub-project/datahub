import React, { useState } from 'react';
import styled from 'styled-components';
import { Modal, Tabs } from 'antd';
import { UnionType } from '../search/utils/constants';
import { FacetFilterInput, TestResultType } from '../../types.generated';
import { EmbeddedListSearch } from '../entity/shared/components/styled/search/EmbeddedListSearch';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const StyledModal = styled(Modal)`
    top: 4vh;
    max-width: 1200px;
`;

const MODAL_WIDTH = '80vw';

const MODAL_BODY_STYLE = {
    padding: 0,
    height: '76vh',
};

const tabBarStyle = { paddingLeft: 28, paddingBottom: 0, marginBottom: 0 };

type Props = {
    urn: string;
    name: string;
    defaultActive?: TestResultType;
    passingCount: number;
    failingCount: number;
    onClose?: () => void;
};

export default function TestResultsModal({
    urn,
    name,
    defaultActive = TestResultType.Success,
    passingCount,
    failingCount,
    onClose,
}: Props) {
    const [resultType, setResultType] = useState(defaultActive);

    // Component state
    const [query, setQuery] = useState<string>('');
    const [page, setPage] = useState(1);
    const [unionType, setUnionType] = useState(UnionType.AND);

    const [filters, setFilters] = useState<Array<FacetFilterInput>>([]);

    const onChangeQuery = (q: string) => {
        setQuery(q);
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        setFilters(newFilters);
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    return (
        <StyledModal
            visible
            width={MODAL_WIDTH}
            title={<>Results - {name}</>}
            closable
            onCancel={onClose}
            bodyStyle={MODAL_BODY_STYLE}
            data-testid="test-results-modal"
            footer={null}
        >
            <Container>
                <Tabs
                    tabBarStyle={tabBarStyle}
                    defaultActiveKey={resultType}
                    activeKey={resultType}
                    size="large"
                    onTabClick={(newType) => setResultType(newType as TestResultType)}
                    onChange={(newType) => setResultType(newType as TestResultType)}
                >
                    <Tabs.TabPane tab={`Passing (${passingCount})`} key={TestResultType.Success} />
                    <Tabs.TabPane tab={`Failing (${failingCount})`} key={TestResultType.Failure} />
                </Tabs>
                <EmbeddedListSearch
                    query={query}
                    filters={filters}
                    page={page}
                    unionType={unionType}
                    onChangeQuery={onChangeQuery}
                    onChangeFilters={onChangeFilters}
                    onChangePage={onChangePage}
                    onChangeUnionType={setUnionType}
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [
                            {
                                field: resultType === TestResultType.Success ? 'passingTests' : 'failingTests',
                                values: [urn],
                            },
                        ],
                    }}
                    placeholderText="Search test results..."
                    defaultShowFilters
                />
            </Container>
        </StyledModal>
    );
}
