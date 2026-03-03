import { SlackOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import {
    BasicDetailsContainer,
    DraftsOutlinedIconStyle,
    EmptyValue,
    SocialDetails,
    SocialInfo,
} from '@app/entityV2/shared/SidebarStyledComponents';

const StyledBasicDetailsContainer = styled(BasicDetailsContainer)`
    padding: 10px;
`;

type Props = {
    email: string | undefined;
    slack: string | undefined;
};

export const GroupBasicInfoSection = ({ email, slack }: Props) => {
    return (
        <StyledBasicDetailsContainer>
            <SocialInfo>
                <SocialDetails>
                    <DraftsOutlinedIconStyle />
                    {email || <EmptyValue />}
                </SocialDetails>
                <SocialDetails>
                    <SlackOutlined />
                    {slack || <EmptyValue />}
                </SocialDetails>
            </SocialInfo>
        </StyledBasicDetailsContainer>
    );
};
