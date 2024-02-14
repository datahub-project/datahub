import React from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { SectionHeader, StyledDivider } from './components';
import StatsCounts from './StatsCounts';

const StatsSectionWrapper = styled.div`
    margin-bottom: 50px;
`;
const Header = styled.div`
    display: flex;
    justify-content: space-between;
`;
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
    if (!fieldProfile || historicFieldProfiles.length <= 1) return null;

    return (
        <StatsSectionWrapper>
            <Header>
                <SectionHeader>Stats</SectionHeader>
                <ViewAll onClick={() => setSelectedTabName('Statistics')}>View all</ViewAll>
            </Header>
            <StatsCounts expandedField={expandedField} fieldProfile={fieldProfile} profiles={historicFieldProfiles} />
            <StyledDivider dashed />
        </StatsSectionWrapper>
    );
}
