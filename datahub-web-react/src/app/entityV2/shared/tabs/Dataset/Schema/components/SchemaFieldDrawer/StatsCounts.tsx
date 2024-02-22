import TrendingDownOutlinedIcon from '@mui/icons-material/TrendingDownOutlined';
import TrendingUpOutlinedIcon from '@mui/icons-material/TrendingUpOutlined';
import { Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { extractChartValuesFromFieldProfiles } from '../../../Stats/historical/HistoricalStats';

const TrendsDetailsContent = styled.div`
    display: flex;
    flex-direction: row;
    gap: 10px;
    margin-top: 15px;
    justify-content: space-between;
    align-items: flex-start;
`;

const TrendDetailContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 5px;
    align-items: center;
`;
const Details = styled.div`
    display: flex;
    flex-direction: column;
`;
const TrendingUpIconContainer = styled.div`
    border-radius: 50%;
    background: ${REDESIGN_COLORS.TERTIARY_GREEN};
    color: ${REDESIGN_COLORS.WHITE};
    height: 30px;
    width: 30px;
    padding: 3px;
`;
const TrendingDownIconContainer = styled.div`
    border-radius: 50%;
    background: #d07b7b;
    color: ${REDESIGN_COLORS.WHITE};
    height: 30px;
    width: 30px;
    padding: 3px;
`;

const TrendingNumber = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-family: Mulish;
    font-size: 12px;
    font-weight: 700;
    line-height: 24px;
`;

const TrendingType = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-family: Mulish;
    font-size: 10px;
    font-weight: 400;
    line-height: 24px;
`;

interface Props {
    expandedField: SchemaField;
    fieldProfile: DatasetFieldProfile | undefined;
    profiles: any[];
}

export default function StatsCounts({ expandedField, fieldProfile, profiles }: Props) {
    const [noOfIncreasingTrends, setNoOfIncreasingTrends] = useState(0);
    const [noOfDecreasingTrends, setNoOfDecreasingTrends] = useState(0);
    const [noOfConstantTrends, setNoOfConstantTrends] = useState(0);

    useEffect(() => {
        let increasingTrends = 0;
        let decreasingTrends = 0;
        let constantTrends = 0;

        if (fieldProfile && profiles.length > 1) {
            Object.entries(fieldProfile).forEach((stat) => {
                if (typeof stat[1] !== 'number') return;

                const statValues = extractChartValuesFromFieldProfiles(profiles, expandedField.fieldPath, stat[0]);
                if (!statValues[1]) return;

                const currentValue = stat[1];
                const lastValue = statValues[1].value;

                if (currentValue > lastValue) {
                    increasingTrends++;
                } else if (currentValue < lastValue) {
                    decreasingTrends++;
                } else constantTrends++;
            });
            setNoOfIncreasingTrends(increasingTrends);
            setNoOfDecreasingTrends(decreasingTrends);
            setNoOfConstantTrends(constantTrends);
        }
    }, [expandedField.fieldPath, fieldProfile, profiles, profiles.length]);

    return (
        <TrendsDetailsContent>
            <TrendDetailContainer>
                <TrendingDownIconContainer>
                    <TrendingDownOutlinedIcon />
                </TrendingDownIconContainer>
                <Details>
                    <TrendingNumber>
                        {`${noOfDecreasingTrends} Stat${noOfDecreasingTrends !== 1 ? 's' : ''}`}
                    </TrendingNumber>
                    <TrendingType>Trending down</TrendingType>
                </Details>
            </TrendDetailContainer>
            <TrendDetailContainer>
                <TrendingUpIconContainer>
                    <TrendingUpOutlinedIcon />
                </TrendingUpIconContainer>
                <Details>
                    <TrendingNumber>
                        {`${noOfIncreasingTrends} Stat${noOfIncreasingTrends !== 1 ? 's' : ''}`}
                    </TrendingNumber>
                    <TrendingType> Trending up </TrendingType>
                </Details>
            </TrendDetailContainer>
            <TrendDetailContainer>
                <Details>
                    <TrendingNumber>
                        {`${noOfConstantTrends} Stat${noOfConstantTrends !== 1 ? 's' : ''}`}
                    </TrendingNumber>
                    <TrendingType> Stable </TrendingType>
                </Details>
            </TrendDetailContainer>
        </TrendsDetailsContent>
    );
}
