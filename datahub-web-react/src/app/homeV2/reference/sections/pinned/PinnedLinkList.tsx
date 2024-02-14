import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { PinnedLink } from './PinnedLink';

const Title = styled.div<{ hasAction: boolean }>`
    ${(props) => props.hasAction && `:hover { cursor: pointer; }`}
    margin-bottom: 8px;
    color: #403d5c;
    font-weight: 600;
    font-size: 16px;
`;

const Links = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const ShowMoreButton = styled.div`
    margin-top: 12px;
    padding: 0px;
    color: ${ANTD_GRAY[7]};
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

type Props = {
    title: string;
    tip?: React.ReactNode;
    links: any[];
    showMore?: boolean;
    showMoreCount?: number;
    onClickMore?: () => void;
    onClickTitle?: () => void;
};

export const PinnedLinkList = ({
    title,
    tip,
    links,
    showMore = false,
    showMoreCount,
    onClickMore,
    onClickTitle,
}: Props) => {
    return (
        <>
            <Title hasAction={onClickTitle !== undefined} onClick={onClickTitle}>
                <Tooltip title={tip} showArrow={false} placement="right">
                    {title}
                </Tooltip>
            </Title>
            <Links>
                {links.map((link) => {
                    return <PinnedLink key={`${title}-${link.link}`} link={link} />;
                })}
            </Links>
            {showMore && (
                <ShowMoreButton onClick={onClickMore}>
                    {(showMoreCount && <>show {showMoreCount} more</>) || <>show more</>}
                </ShowMoreButton>
            )}
        </>
    );
};
