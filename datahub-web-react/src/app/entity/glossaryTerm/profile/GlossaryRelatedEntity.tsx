import { Col, Row } from 'antd';
import * as React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearchSection } from '../../shared/components/styled/search/EmbeddedListSearchSection';

import { useEntityData } from '../../shared/EntityContext';

const GroupAssetsWrapper = styled(Row)`
    height: 100%;
`;

export default function GlossaryRelatedEntity() {
    const { entityData }: any = useEntityData();
    const glossaryTermHierarchicalName = entityData?.hierarchicalName;
    let fixedQueryString = `glossaryTerms:"${glossaryTermHierarchicalName}" OR fieldGlossaryTerms:"${glossaryTermHierarchicalName}" OR editedFieldGlossaryTerms:"${glossaryTermHierarchicalName}"`;
    entityData?.isAChildren?.relationships.forEach((term) => {
        const name = term.entity?.hierarchicalName;
        fixedQueryString += `OR glossaryTerms:"${name}" OR fieldGlossaryTerms:"${name}" OR editedFieldGlossaryTerms:"${name}"`;
    });

    return (
        <GroupAssetsWrapper>
            <Col md={24} lg={24} xl={24}>
                <EmbeddedListSearchSection
                    fixedQuery={fixedQueryString}
                    emptySearchQuery="*"
                    placeholderText="Filter entities..."
                />
            </Col>
        </GroupAssetsWrapper>
    );
}
