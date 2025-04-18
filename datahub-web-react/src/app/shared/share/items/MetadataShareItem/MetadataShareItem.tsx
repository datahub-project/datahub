import { SendOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';

import ShareModal from '@app/shared/share/items/MetadataShareItem/ShareModal';
import { TextSpan } from '@app/shared/share/items/components';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';

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
