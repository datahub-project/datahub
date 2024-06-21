import React from 'react';
import { SlackOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import {
    EmptyValue,
    SocialDetails,
    BasicDetailsContainer,
    SocialInfo,
    DraftsOutlinedIconStyle,
} from '../shared/SidebarStyledComponents';

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
