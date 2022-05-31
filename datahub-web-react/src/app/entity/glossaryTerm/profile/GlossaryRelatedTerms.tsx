import { Menu } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../shared/EntityContext';
import GlossaryRelatedTermsResult from './GlossaryRelatedTermsResult';

enum RelatedTermsTypes {
    CONTAINS,
    INHERITS,
}

const DetailWrapper = styled.div`
    display: inline-flex;
    width: 100%;
`;

const MenuWrapper = styled.div`
    border: 2px solid #f5f5f5;
`;

const Content = styled.div`
    margin-left: 32px;
    flex-grow: 1;
`;

export default function GlossayRelatedTerms() {
    const { entityData } = useEntityData();
    const [selectedType, setSelectedType] = useState<RelatedTermsTypes>(RelatedTermsTypes.CONTAINS);
    const relatedTermUrns = new Set<string>();

    if (selectedType === RelatedTermsTypes.CONTAINS) {
        entityData?.hasRelatedTerms?.relationships.forEach((term) => relatedTermUrns.add(term.entity?.urn || ''));
        entityData?.childTerms?.relationships.forEach((term) => relatedTermUrns.add(term.entity?.urn || ''));
    } else {
        entityData?.isRelatedTerms?.relationships.forEach((term) => relatedTermUrns.add(term.entity?.urn || ''));
        entityData?.parentTerms?.relationships.forEach((term) => relatedTermUrns.add(term.entity?.urn || ''));
    }

    return (
        <DetailWrapper>
            <MenuWrapper>
                <Menu selectable={false} mode="inline" style={{ width: 256 }} selectedKeys={[selectedType.toString()]}>
                    <Menu.Item
                        data-testid="hasRelatedTerms"
                        key={RelatedTermsTypes.CONTAINS}
                        onClick={() => setSelectedType(RelatedTermsTypes.CONTAINS)}
                    >
                        Contains
                    </Menu.Item>
                    <Menu.Item
                        data-testid="isRelatedTerms"
                        key={RelatedTermsTypes.INHERITS}
                        onClick={() => setSelectedType(RelatedTermsTypes.INHERITS)}
                    >
                        Inherits
                    </Menu.Item>
                </Menu>
            </MenuWrapper>
            <Content>
                {entityData && (
                    <GlossaryRelatedTermsResult
                        glossaryRelatedTermType={selectedType === RelatedTermsTypes.CONTAINS ? 'Contains' : 'Inherits'}
                        glossaryRelatedTermUrns={Array.from(relatedTermUrns) || []}
                    />
                )}
            </Content>
        </DetailWrapper>
    );
}
