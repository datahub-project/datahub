import { Button, Divider, Tooltip } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';
import SidebarBackArrow from '../../../../../../images/sidebarBackArrow.svg?react';
import EntitySidebarContext from '../../../../../shared/EntitySidebarContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';
import { ANTD_GRAY, SEARCH_COLORS } from '../../../constants';

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    height: 40px;
    padding: 8px;
`;

const CloseButton = styled(Button)<{ isActive }>`
    margin: 0px;
    padding: 2px 6px;
    display: flex;
    align-items: center;
    && {
        color: ${(props) => (props.isActive ? ANTD_GRAY[9] : ANTD_GRAY[8])};
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const StyledSidebarBackArrow = styled(SidebarBackArrow)<{ direction: 'left' | 'right' }>`
    cursor: pointer;
    ${(props) => (props.direction === 'left' && 'transform: scaleX(-1);') || undefined}
`;

interface Props {
    hideCollapseViewDetails: boolean;
}

export default function SidebarCollapseControls({ hideCollapseViewDetails }: Props) {
    const entityRegistry = useEntityRegistry();
    const { urn, entityType } = useEntityData();
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    const showViewDetails = !hideCollapseViewDetails && !isClosed;

    return (
        <>
            <Controls isCollapsed={isClosed}>
                <Tooltip placement="left" showArrow={false} title={!isClosed ? 'Close sidebar' : 'Open sidebar'}>
                    <CloseButton isActive={!isClosed} type="link" onClick={() => setSidebarClosed(!isClosed)}>
                        <StyledSidebarBackArrow direction={isClosed ? 'left' : 'right'} />
                    </CloseButton>
                </Tooltip>
                {showViewDetails && (
                    <Button
                        size="small"
                        type="primary"
                        style={{ backgroundColor: SEARCH_COLORS.TITLE_PURPLE, border: 'none' }}
                        href={entityRegistry.getEntityUrl(entityType, urn)}
                    >
                        View more
                    </Button>
                )}
            </Controls>
            <ThinDivider />
        </>
    );
}
