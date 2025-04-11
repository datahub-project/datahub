import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';

export const FilterContainer = styled.div<{ $isCompact: boolean; isDisabled?: boolean }>`
    box-shadow: 0px 0px 4px 0px #00000010;
    border-radius: 10px;
    border: 1px solid ${ANTD_GRAY[4]};
    padding: ${(props) => (props.$isCompact ? '0 4px' : '4px')};
    margin: ${(props) => (props.$isCompact ? '2px 4px 2px 4px' : '4px')};

    ${(props) =>
        props.isDisabled
            ? `background: ${ANTD_GRAY[4]};`
            : `
            :hover {
                cursor: pointer;
                background: ${ANTD_GRAY[2]};
            }
    `}
`;
