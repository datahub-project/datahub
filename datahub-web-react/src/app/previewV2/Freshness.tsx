import { green, orange } from '@ant-design/colors';
import UpdateOutlinedIcon from '@mui/icons-material/UpdateOutlined';
import { Popover } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';
import { getLastIngestedColor } from '../entity/shared/containers/profile/sidebar/LastIngested';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const LastUpdatedContainer = styled.div<{ color: string }>`
    align-items: center;
    color: ${ANTD_GRAY[7]};
    display: flex;
    flex-direction: row;
    gap: 5px;
    svg {
        font-size: 16px;
        color: ${(props) => props.color};
    }
`;

const PopoverContent = styled.div`
    align-items: center;
    display: flex;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

type Props = {
    time: number; // Milliseconds
};

const Freshness = ({ time }: Props) => {
    const lastIngestedColor = getLastIngestedColor(time);
    let lastUpdatedText;
    if (lastIngestedColor === green[5]) lastUpdatedText = 'Updated in the past week';
    else if (lastIngestedColor === orange[5]) lastUpdatedText = 'Updated in the past month';
    else lastUpdatedText = 'Updated more than a month ago';

    return (
        <Popover content={<PopoverContent>{lastUpdatedText}</PopoverContent>} placement="bottom" showArrow={false}>
            <LastUpdatedContainer color={getLastIngestedColor(time)}>
                <UpdateOutlinedIcon />
            </LastUpdatedContainer>
        </Popover>
    );
};

export default Freshness;
