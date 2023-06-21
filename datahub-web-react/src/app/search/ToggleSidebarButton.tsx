import React, { memo } from 'react';
import Icon from '@ant-design/icons/lib/components/Icon';
import { Button } from 'antd';
import styled from 'styled-components';
import { ReactComponent as ExpandIcon } from '../../images/expand.svg';
import { ReactComponent as CollapseIcon } from '../../images/collapse.svg';

const ToggleIcon = styled(Icon)`
    color: ${(props) => props.theme.styles['primary-color']};
    &&& {
        font-size: 16px;
    }
`;

const ToggleSidebarButton = ({ isOpen, onClick }: { isOpen: boolean; onClick: () => void }) => {
    return (
        <Button size="small" onClick={onClick} icon={<ToggleIcon component={isOpen ? CollapseIcon : ExpandIcon} />} />
    );
};

export default memo(ToggleSidebarButton);
