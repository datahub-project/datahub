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

const NumberText = styled(Typography.Text)`
    color: #5b6282;
    font-weight: 700;
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
                collapsedContent={
                    sortedResults.length > 1 ? (
                        <SharingInfo>
                            <InfoText> Shared with </InfoText>
                            <NumberText>{`${sortedResults.length} Acryl ${pluralize(
                                sortedResults.length,
                                'Instance',
                            )}`}</NumberText>
                        </SharingInfo>
                    ) : (
                        <SharingContainer>
                            <SharingList resultsList={sortedResults} />
                        </SharingContainer>
                    )
                }
                collapsible={sortedResults.length !== 1}
                expandedByDefault={sortedResults.length === 1}
                content={<>{sortedResults.length > 1 && <SharingList resultsList={sortedResults} />}</>}
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
