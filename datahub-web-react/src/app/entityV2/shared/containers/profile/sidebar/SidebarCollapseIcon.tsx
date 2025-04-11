import { Tooltip } from '@components';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { ArrowLeft, ArrowRight } from '@phosphor-icons/react';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../../constants';

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 56px;
    padding: 8px;
`;

const IconWrapper = styled.div<{ direction: 'left' | 'right' }>`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const CloseButton = styled.div<{ $isClosed: boolean }>`
    cursor: pointer;
    margin: 0px;
    padding: 2px 6px;
    display: flex;
    align-items: center;
    height: 40px;
    width: 40px;
    border-radius: 6px;
    justify-content: center;
    color: ${SEARCH_COLORS.TITLE_PURPLE};
    ${(props) =>
        props.$isClosed &&
        `
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE_2};
        color: ${REDESIGN_COLORS.WHITE};  
    `}
    :hover {
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE_2};
        color: ${REDESIGN_COLORS.WHITE};
    }
`;

export default function SidebarCollapseIcon() {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    return (
        <Container>
            <Tooltip placement="left" showArrow={false} title={!isClosed ? 'Close sidebar' : 'Open sidebar'}>
                <CloseButton
                    $isClosed={isClosed}
                    onClick={() => setSidebarClosed(!isClosed)}
                    data-testid="toggleSidebar"
                >
                    <IconWrapper direction={isClosed ? 'left' : 'right'}>
                        {isClosed ? <ArrowRight size={20} /> : <ArrowLeft size={20} />}
                    </IconWrapper>
                </CloseButton>
            </Tooltip>
        </Container>
    );
}
