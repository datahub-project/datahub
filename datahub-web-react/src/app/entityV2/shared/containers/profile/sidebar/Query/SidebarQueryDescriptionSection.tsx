import React from 'react';
import styled from 'styled-components/macro';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { QueryEntity } from '@types';

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
