import React, { useContext, useEffect, useState } from 'react';
import styled from 'styled-components';
import { Carousel, Button } from 'antd';
import { Tooltip } from '@components';
import { NotificationOutlined, CloseOutlined } from '@ant-design/icons';
import AnnouncementsSkeleton from '../../content/tabs/announcements/AnnouncementsSkeleton';
import { Announcement } from './Announcement';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { useUserContext } from '../../../context/useUserContext';
import { useUpdateLastViewedAnnouncementTime } from '../../shared/updateLastViewedAnnouncementTime';
import { useGetUnseenAnnouncements } from './useGetUnseenAnnouncements';
import OnboardingContext from '../../../onboarding/OnboardingContext';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 11px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 16px 20px 8px 20px;
    width: 380px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const Title = styled.div`
    font-weight: 600;
    font-size: 14px;
    color: #434863;
    display: flex;
    align-items: center;
    justify-content: start;
`;

const Icon = styled(NotificationOutlined)`
    margin-right: 8px;
    color: #3cb47a;
    font-size: 16px;
`;

const StyledCloseOutlined = styled(CloseOutlined)`
    color: ${ANTD_GRAY[8]};
    font-size: 12px;
`;

const StyledCarousel = styled(Carousel)`
    padding: 12px 0px 20px 0px;
    font-weight: 600;
    font-size: 14px;
    overflow: hidden;

    > .slick-dots li button {
        background-color: #d9d9d9;
    }

    > .slick-dots li.slick-active button {
        background-color: #5c3fd1;
    }
`;

const CloseButton = styled(Button)`
    margin: 0px;
    padding: 2px;
`;

type Props = {
    setHasAnnouncements?: (value: boolean) => void;
};

export const Announcements = ({ setHasAnnouncements }: Props) => {
    const { user } = useUserContext();
    const { announcements, loading } = useGetUnseenAnnouncements();
    const { updateLastViewedAnnouncementTime } = useUpdateLastViewedAnnouncementTime();

    const { isUserInitializing } = useContext(OnboardingContext);

    const sortedAnnouncements = announcements.sort((a, b) => {
        return b?.lastModified?.time - a?.lastModified?.time;
    });

    const [hidden, setHidden] = useState(false);

    const hideAnnouncements = () => {
        if (!user?.urn) return;
        updateLastViewedAnnouncementTime(user?.urn);
        setHidden(true);
    };

    useEffect(() => {
        setHasAnnouncements?.(!hidden && !!sortedAnnouncements.length);
    }, [setHasAnnouncements, hidden, sortedAnnouncements.length]);

    if (hidden || !sortedAnnouncements.length) {
        return null;
    }

    if (isUserInitializing || loading) {
        return (
            <Card>
                <AnnouncementsSkeleton />
            </Card>
        );
    }

    return (
        <Card>
            <Header>
                <Title>
                    <Icon /> Announcements
                </Title>
                <Tooltip placement="left" showArrow={false} title="Hide announcements">
                    <CloseButton type="text" onClick={hideAnnouncements}>
                        <StyledCloseOutlined />
                    </CloseButton>
                </Tooltip>
            </Header>
            <StyledCarousel autoplaySpeed={8000} autoplay>
                {sortedAnnouncements.map((announcement) => (
                    <Announcement
                        key={`${announcement.content.title}-${announcement.content.description}`}
                        announcement={announcement}
                    />
                ))}
            </StyledCarousel>
        </Card>
    );
};
