import React, { useState } from 'react';
import styled from 'styled-components';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Typography } from 'antd';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { SidebarSection } from '../SidebarSection';
import { sortSharedList } from '../../../../../../entity/shared/containers/profile/utils';
import { pluralize } from '../../../../../../shared/textUtil';
import SectionActionButton from '../SectionActionButton';
import ShareModal from '../../../../../../shared/share/v2/items/MetadataShareItem/ShareModal';
import SharingList from './SharingList';
import { REDESIGN_COLORS } from '../../../../constants';
import SharedByInfo from './SharedByInfo';

const SharingInfo = styled.div`
    margin: 5px 0;
`;

const SharingContainer = styled.div`
    margin-left: 23px;
`;

const InfoText = styled(Typography.Text)`
    color: #5b6282;
    font-weight: 500;
`;

const Header = styled.div`
    font-weight: 700;
    margin-top: 10px;
    font-size: 14px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    display: flex;
    align-items: center;
`;

const NumberText = styled(Typography.Text)`
    color: #5b6282;
    font-weight: 700;
`;

const SharingAssetSection = () => {
    const { entityData } = useEntityData();

    const lastShareResults = entityData?.share?.lastShareResults;
    const sortedResults = lastShareResults && sortSharedList(lastShareResults);
    const directShares = sortedResults?.filter((result) => !result.implicitShareEntity);
    const implicitShareResults = sortedResults?.filter((result) => !!result.implicitShareEntity);
    const distinctInstances = new Set(lastShareResults?.map((result) => result.destination?.urn));

    const [isShareModalVisible, setIsShareModalVisible] = useState(false);

    return (
        <>
            {sortedResults && sortedResults.length > 0 && (
                <SidebarSection
                    title="Sharing"
                    collapsedContent={
                        sortedResults.length > 1 ? (
                            <SharingInfo>
                                <InfoText> Shared with </InfoText>
                                <NumberText>
                                    {`${distinctInstances.size} Acryl ${pluralize(distinctInstances.size, 'Instance')}`}
                                </NumberText>
                            </SharingInfo>
                        ) : (
                            <SharingContainer>
                                <SharingList resultsList={sortedResults} />
                            </SharingContainer>
                        )
                    }
                    collapsible={sortedResults.length !== 1}
                    expandedByDefault={sortedResults.length === 1}
                    content={
                        <>
                            {sortedResults.length > 1 && directShares && directShares?.length > 0 && (
                                <SharingList resultsList={directShares} />
                            )}

                            {sortedResults.length > 1 && implicitShareResults && implicitShareResults?.length > 0 && (
                                <>
                                    <Header>
                                        Shared by &nbsp;
                                        <SharedByInfo />
                                    </Header>
                                    <SharingList resultsList={implicitShareResults} />
                                </>
                            )}
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
            )}
            <ShareModal isModalVisible={isShareModalVisible} closeModal={() => setIsShareModalVisible(false)} />
        </>
    );
};

export default SharingAssetSection;
