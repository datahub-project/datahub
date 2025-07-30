import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType, HomePageModule } from '@app/analytics';
import { PinnedLink } from '@app/homeV2/reference/sections/pinned/PinnedLink';
import { Carousel } from '@src/app/sharedV2/carousel/Carousel';

import { PostContent } from '@types';

const Title = styled.div<{ hasAction: boolean }>`
    ${(props) => props.hasAction && `:hover { cursor: pointer; }`}
    margin-bottom: 8px;
    color: #403d5c;
    font-weight: 600;
    font-size: 16px;
`;

type Props = {
    title?: string;
    tip?: React.ReactNode;
    links: PostContent[];
    onClickTitle?: () => void;
};

export const PinnedLinkList = ({ title, tip, links, onClickTitle }: Props) => {
    function handleLinkClick(link: PostContent) {
        analytics.event({
            type: EventType.HomePageClick,
            module: HomePageModule.Discover,
            section: 'Pinned Links',
            value: link.title,
        });
    }

    return (
        <>
            {title ? (
                <Title hasAction={onClickTitle !== undefined} onClick={onClickTitle}>
                    <Tooltip title={tip} showArrow={false} placement="right">
                        {title}
                    </Tooltip>
                </Title>
            ) : null}
            <Carousel>
                {links.map((link) => {
                    return (
                        // eslint-disable-next-line
                        <span key={`${title}-${link.link}`} onClick={() => handleLinkClick(link)}>
                            <PinnedLink link={link} />
                        </span>
                    );
                })}
            </Carousel>
        </>
    );
};
