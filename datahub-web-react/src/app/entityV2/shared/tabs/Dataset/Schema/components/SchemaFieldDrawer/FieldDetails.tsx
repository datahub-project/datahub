import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
// import DeprecationIcon from '../../../../../../../../images/announcement-icon.svg?react';
import { UsageQueryResult } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { FieldPopularity } from './FieldPopularity';

const FieldDetailsWrapper = styled.div`
    padding: 16px 24px;
    background: rgba(217, 217, 217, 0.2);
    margin-bottom: 24px;
`;
const FieldDetailsContent = styled.div`
    display: flex;
    flex-direction: row;
    gap: 10px;
`;

const PopularityContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 2;
    gap: 5px;
`;
/* const IncidentContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 2;
    gap: 5px;
`;
const DeprecationContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 3;
`;

const Deprecation = styled.div`
    display: flex;
    flex-direction: row;
    gap: 10px;
    svg {
        height: 25px;
        width: 25px;
    }
`; */

const DetailLabel = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;
`;

const DetailValue = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    opacity: 0.5;
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;
    width: max-content;
`;

type FieldDetailsProps = {
    fieldPath: string | null;
    usageStats?: UsageQueryResult | null;
};

export const FieldDetails = ({ fieldPath, usageStats }: FieldDetailsProps) => {
    return (
        <FieldDetailsWrapper>
            <FieldDetailsContent>
                <PopularityContainer>
                    <DetailLabel>Popularity</DetailLabel>
                    <DetailValue>
                        <FieldPopularity
                            isFieldSelected={false}
                            usageStats={usageStats}
                            fieldPath={fieldPath}
                            displayOnDrawer
                        />
                    </DetailValue>
                </PopularityContainer>
                {/* OBS: 489: We don't yet support incidents or deprecation on the schema fields yet. So made it invisible for the beta release. */}
                {/* <IncidentContainer>
                    <DetailLabel>Incident </DetailLabel>
                    <DetailValue> - - </DetailValue>
                </IncidentContainer>
                <DeprecationContainer>
                    <Deprecation>
                        <DetailLabel>Deprecation </DetailLabel>
                        <DeprecationIcon />
                    </Deprecation>
                    <DetailValue> - - </DetailValue>
                </DeprecationContainer> */}
            </FieldDetailsContent>
        </FieldDetailsWrapper>
    );
};
