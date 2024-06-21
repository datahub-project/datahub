import React from 'react';

import styled from 'styled-components/macro';
import { Button, Badge } from 'antd';
import { HiOutlineClipboardList } from 'react-icons/hi';
import { useHistory } from 'react-router-dom';

import { useUserContext } from '../../context/useUserContext';
import { PageRoutes } from '../../../conf/Global';
import analytics, { EventType } from '../../analytics';

const LinkWrapper = styled.span`
    margin-right: 0px;
`;

const InnerButtonWrapper = styled.div`
    position: relative;
    display: flex;
    align-items: center;

    svg {
        margin-right: 0.35rem;
        height: 18px;
        width: 18px;
        path {
            stroke-width: 1.6;
        }
    }
`;

const badgeBoxSize = '14px';

const StyledBadge = styled(Badge)`
    position: absolute;
    bottom: 0;
    left: 0.4rem;

    sup {
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 100%;
        padding: 0;
        font-size: 9px;
        font-weight: bold;
        min-width: ${badgeBoxSize};
        width: ${badgeBoxSize};
        height: ${badgeBoxSize};
        line-height: ${badgeBoxSize};
    }
`;

export const TaskCenterLink = () => {
    const history = useHistory();
    const {
        state: { unfinishedTaskCount },
    } = useUserContext();
    const showBadge = unfinishedTaskCount > 0;

    const goToTaskCenter = () => {
        history.push(PageRoutes.ACTION_REQUESTS);
        analytics.event({
            type: EventType.OpenTaskCenter,
        });
    };

    return (
        <LinkWrapper>
            <Button type="text" onClick={goToTaskCenter}>
                <InnerButtonWrapper>
                    {showBadge && (
                        <StyledBadge count={unfinishedTaskCount} overflowCount={99} size="small" color="red" />
                    )}
                    <HiOutlineClipboardList size="16px" />
                    Tasks
                </InnerButtonWrapper>
            </Button>
        </LinkWrapper>
    );
};
