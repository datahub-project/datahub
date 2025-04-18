import { SendOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React, { useState } from 'react';
import ShareModal from './ShareModal';
import { StyledMenuItem } from '../../styledComponents';

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
