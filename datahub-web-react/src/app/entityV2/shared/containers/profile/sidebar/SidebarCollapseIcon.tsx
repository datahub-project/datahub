import { Tooltip } from '@components';
import React, { useContext } from 'react';
import styled from 'styled-components';
import KeyboardTabOutlinedIcon from '@mui/icons-material/KeyboardTabOutlined';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../../constants';

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 56px;
    padding: 8px;
`;

const StyledKeyboardTabOutlinedIcon = styled(KeyboardTabOutlinedIcon)<{ direction: 'left' | 'right' }>`
    ${(props) => (props.direction === 'left' && 'transform: scaleX(-1);') || undefined}
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
                    <StyledKeyboardTabOutlinedIcon direction={isClosed ? 'left' : 'right'} />
                </CloseButton>
            </Tooltip>
        </Container>
    );
}
