import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import StatsSummaryRow from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSummaryRow';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';

import { DatasetFieldProfile } from '@types';

const ViewAll = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-family: Mulish;
    font-size: 10px;
    font-weight: 400;
    line-height: 24px;
    :hover {
        cursor: pointer;
    }
`;

interface Props {
    fieldProfile: DatasetFieldProfile | undefined;
    setSelectedTabName: any;
}

export default function StatsSection({ fieldProfile, setSelectedTabName }: Props) {
    // If current field profile doesn't exist or historic profiles don't have multiple profiles of the current field
    if (!fieldProfile) return null;

    return (
        <>
            <SidebarSection
                title="Stats"
                extra={<ViewAll onClick={() => setSelectedTabName('Statistics')}>View all</ViewAll>}
                content={<StatsSummaryRow fieldProfile={fieldProfile} />}
            />
            <StyledDivider dashed />
        </>
    );
}
