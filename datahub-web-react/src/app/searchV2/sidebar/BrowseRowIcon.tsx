import styled from 'styled-components';

import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

/** Platform logos sit at this size inside the tree row's icon box, matching Metrics / Documents. */
export const BROWSE_ROW_LOGO_SIZE = 16;

/** Fixed icon box for Navigate tree rows, sized like the other browse sidebars' row icons. */
const BrowseRowIcon = styled.div<{ $isSelected?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: ${TREE_ROW_ENTITY_ICON_SIZE}px;
    height: ${TREE_ROW_ENTITY_ICON_SIZE}px;
    flex-shrink: 0;

    && svg {
        color: ${(props) => (props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.icon)};
    }
`;

export default BrowseRowIcon;
