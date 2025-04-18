import { SendOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';

import ShareModal from '@app/shared/share/v2/items/MetadataShareItem/ShareModal';
import { StyledMenuItem } from '@app/shared/share/v2/styledComponents';

interface Props {
    key: string;
}

export const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
    font-weight: normal;
`;

export default function MetadataShareItem({ key }: Props) {
    const [isShareModalVisible, setIsShareModalVisible] = useState(false);

    return (
        <>
            <StyledMenuItem key={key} onClick={() => setIsShareModalVisible(true)}>
                <SendOutlined />
                <TextSpan>Share with another instance</TextSpan>
            </StyledMenuItem>
            <ShareModal isModalVisible={isShareModalVisible} closeModal={() => setIsShareModalVisible(false)} />
        </>
    );
}
