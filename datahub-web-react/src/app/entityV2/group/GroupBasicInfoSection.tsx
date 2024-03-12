import React from 'react';
import { MailOutlined, SlackOutlined } from '@ant-design/icons';
import { EmptyValue, SocialDetails, BasicDetailsContainer, SocialInfo } from '../shared/SidebarStyledComponents';

type Props = {
    email: string | undefined;
    slack: string | undefined;
};

export const GroupBasicInfoSection = ({ email, slack }: Props) => {
    return (
        <BasicDetailsContainer>
            <SocialInfo>
                <SocialDetails>
                    <MailOutlined />
                    {email || <EmptyValue />}
                </SocialDetails>
                <SocialDetails>
                    <SlackOutlined />
                    {slack || <EmptyValue />}
                </SocialDetails>
            </SocialInfo>
        </BasicDetailsContainer>
    );
};
