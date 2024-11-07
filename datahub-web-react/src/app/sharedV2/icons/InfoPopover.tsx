import { InfoCircleFilled, InfoCircleOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { Popover } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

const InfoWrapper = styled.div`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
`;

interface Props {
    content: React.ReactNode;
    className?: string;
}

export default function InfoPopover({ content, className }: Props) {
    const [showPopover, setShowPopover] = useState(false);

    return (
        <InfoWrapper className={className}>
            <Popover placement="top" content={content} trigger="hover" open={showPopover} onOpenChange={setShowPopover}>
                {showPopover ? <InfoCircleFilled /> : <InfoCircleOutlined />}
            </Popover>
        </InfoWrapper>
    );
}
