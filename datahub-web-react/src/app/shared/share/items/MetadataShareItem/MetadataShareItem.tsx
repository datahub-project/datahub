import { SendOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import styled from 'styled-components';
import React, { useState } from 'react';
import { TextSpan } from '../components';
import ShareModal from './ShareModal';

const StyledMenuItem = styled.div`
    && {
        color: ${ANTD_GRAY[8]};
    }
`;

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
