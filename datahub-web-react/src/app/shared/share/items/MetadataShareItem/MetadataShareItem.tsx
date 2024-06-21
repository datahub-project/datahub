import { SendOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import { StyledMenuItem, TextSpan } from '../components';
import ShareModal from './ShareModal';

interface Props {
    key: string;
}

export default function MetadataShareItem({ key }: Props) {
    const [isShareModalVisible, setIsShareModalVisible] = useState(false);

    return (
        <>
            <StyledMenuItem key={key} onClick={() => setIsShareModalVisible(true)}>
                <SendOutlined />
                <TextSpan>
                    <strong>Send to another instance</strong>
                </TextSpan>
            </StyledMenuItem>
            <ShareModal isModalVisible={isShareModalVisible} closeModal={() => setIsShareModalVisible(false)} />
        </>
    );
}
