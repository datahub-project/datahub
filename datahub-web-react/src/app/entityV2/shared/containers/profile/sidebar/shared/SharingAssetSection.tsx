import React, { useState } from 'react';
import styled from 'styled-components';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Typography } from 'antd';
import moment from 'moment-timezone';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { REDESIGN_COLORS } from '../../../../constants';
import { SidebarSection } from '../SidebarSection';
import { sortSharedList } from '../../../../../../entity/shared/containers/profile/utils';
import { pluralize } from '../../../../../../shared/textUtil';
import AcrylIcon from '../../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../../images/share-icon-custom.svg?react';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';
import SectionActionButton from '../SectionActionButton';
import ShareModal from '../../../../../../shared/share/v2/items/MetadataShareItem/ShareModal';
import { ContentText, InstanceIcon, LabelText, RelativeTime } from './styledComponents';

const DetailsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-direction: column;
`;

const DetailRow = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
`;

const UpdatedRow = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    margin-left: 22px;
`;

const SharingInfo = styled.div`
    margin-bottom: 10px;
`;

const InfoText = styled(Typography.Text)`
    color: #5b6282;
    font-weight: 500;
`;

const NumberText = styled(Typography.Text)`
    color: #5b6282;
    font-weight: 700;
`;

const StyledShareIcon = styled(ShareIcon)`
    height: 18px;
    width: 18px;
    path {
        stroke: ${REDESIGN_COLORS.BODY_TEXT};
    }
`;

const ResultsContainer = styled.div`
    & > div {
        padding: 12px 0;
        &:not(:last-child) {
            border-bottom: 1px dashed;
            border-color: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
        }
    }
`;

const SharingAssetSection = () => {
    const { entityData } = useEntityData();

    const lastShareResults = entityData?.share?.lastShareResults?.filter((result) => !!result.lastSuccess?.time);
    const [isShareModalVisible, setIsShareModalVisible] = useState(false);
    if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;
    const sortedResults = sortSharedList(lastShareResults);

    return (
        <>
            <SidebarSection
                title="Sharing"
                content={
                    <>
                        <SharingInfo>
                            <InfoText> Shared with </InfoText>
                            <NumberText>
                                {`${sortedResults.length} Acryl ${pluralize(sortedResults.length, 'Instance')}`}
                            </NumberText>
                        </SharingInfo>
                        <ResultsContainer>
                            {sortedResults.map((result) => {
                                const name = result.destination.details.name || result.destination.urn;
                                const lastSuccessTime = result.lastSuccess?.time || 0;
                                const isRecentlyUpdated = moment(lastSuccessTime).isAfter(moment().subtract(1, 'week'));

                                return (
                                    <DetailsContainer>
                                        <DetailRow>
                                            <StyledShareIcon />
                                            <LabelText>To: </LabelText>
                                            <InstanceIcon>
                                                <AcrylIcon />
                                            </InstanceIcon>
                                            <ContentText>{name}</ContentText>
                                        </DetailRow>
                                        <UpdatedRow>
                                            <LabelText>Date Updated: </LabelText>
                                            <ContentText>{toLocalDateString(lastSuccessTime)}</ContentText>
                                            <RelativeTime isRecentlyUpdated={isRecentlyUpdated}>
                                                {toRelativeTimeString(lastSuccessTime)}
                                            </RelativeTime>
                                        </UpdatedRow>
                                    </DetailsContainer>
                                );
                            })}
                        </ResultsContainer>
                    </>
                }
                extra={
                    <>
                        <SectionActionButton
                            button={<EditOutlinedIcon />}
                            onClick={(event) => {
                                setIsShareModalVisible(true);
                                event.stopPropagation();
                            }}
                        />
                    </>
                }
            />
            <ShareModal isModalVisible={isShareModalVisible} closeModal={() => setIsShareModalVisible(false)} />
        </>
    );
};

export default SharingAssetSection;
