import { Col, Row } from 'antd';
import * as React from 'react';
import { EmbeddedListSearch } from '../../shared/components/styled/search/EmbeddedListSearch';

import { useEntityData } from '../../shared/EntityContext';

export default function GlossaryRelatedEntity() {
    const { entityData }: any = useEntityData();
    const glossaryTermHierarchicalName = entityData?.hierarchicalName;
    const fixedQueryString = `glossaryTerms:"${glossaryTermHierarchicalName}" OR fieldGlossaryTerms:"${glossaryTermHierarchicalName}" OR editedFieldGlossaryTerms:"${glossaryTermHierarchicalName}"`;

    return (
        <Row>
            <Col md={24} lg={24} xl={24}>
                <EmbeddedListSearch
                    style={{ height: 336 }}
                    fixedQuery={fixedQueryString}
                    emptySearchQuery="*"
                    placeholderText="Filter entities..."
                />
            </Col>
        </Row>
    );
}
