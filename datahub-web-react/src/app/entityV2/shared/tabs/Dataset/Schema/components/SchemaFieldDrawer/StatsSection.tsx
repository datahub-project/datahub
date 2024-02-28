import React from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { SidebarSection } from '../../../../../containers/profile/sidebar/SidebarSection';
import StatsSummaryRow from './StatsSummaryRow';
import { StyledDivider } from './components';

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
    expandedField: SchemaField;
    fieldProfile: DatasetFieldProfile | undefined;
    profiles: any[];
    setSelectedTabName: any;
}

export default function StatsSection({ expandedField, fieldProfile, profiles, setSelectedTabName }: Props) {
    const historicFieldProfiles = profiles.filter((profile) =>
        profile.fieldProfiles?.some((fieldProf) => fieldProf.fieldPath === expandedField.fieldPath),
    );

    // If current field profile doesn't exist or historic profiles don't have multiple profiles of the current field
    if (!fieldProfile) return null;

    return (
        <>
            <SidebarSection
                title="Stats"
                extra={<ViewAll onClick={() => setSelectedTabName('Statistics')}>View all</ViewAll>}
                content={
                    <StatsSummaryRow
                        expandedField={expandedField}
                        fieldProfile={fieldProfile}
                        profiles={historicFieldProfiles}
                    />
                }
            />
            <StyledDivider dashed />
        </>
    );
}
