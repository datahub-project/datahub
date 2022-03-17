import React from 'react';
import styled from 'styled-components';
import { Col, Row } from 'antd';
import { EmbeddedListSearch } from '../../entity/shared/components/styled/search/EmbeddedListSearch';

const GroupAssetsWrapper = styled(Row)`
    height: calc(100vh - 245px);
    overflow: auto;
`;

type Props = {
    fixedQuery?: string | null;
};

export default function RelatedEntityResults({ fixedQuery }: Props) {
    return (
        <>
            <GroupAssetsWrapper>
                <Col md={24} lg={24} xl={24}>
                    <EmbeddedListSearch
                        fixedQuery={fixedQuery}
                        emptySearchQuery="*"
                        placeholderText="Filter entities..."
                    />
                </Col>
            </GroupAssetsWrapper>
        </>
    );
}
