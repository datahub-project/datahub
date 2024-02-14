import React from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity } from '../../../../EntityContext';
import { SidebarSection } from '../SidebarSection';
import { QueryEntity } from '../../../../../../../types.generated';

const ContentWrapper = styled.div`
    font-size: 12px;
`;

export default function SidebarQueryDescriptionSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();

    if (!baseEntity?.entity?.properties?.description) {
        return null;
    }

    return (
        <SidebarSection
            title="Description"
            content={
                <>
                    <ContentWrapper>{baseEntity?.entity?.properties?.description}</ContentWrapper>
                </>
            }
        />
    );
}
