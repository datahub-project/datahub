import React from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity } from '../../../../../../entity/shared/EntityContext';
import { SidebarSection } from '../SidebarSection';
import { QueryEntity } from '../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../constants';

const ContentWrapper = styled.div`
    font-size: 12px;
    color: ${REDESIGN_COLORS.DARK_GREY};
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
