import { ArrowLeftOutlined, ArrowRightOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../constants';
import BackgroundDots from '../../../../../images/background_dots.svg';

export const BulkNavigationWrapper = styled.div<{ $hideBackground?: boolean }>`
    padding: 16px 68px 16px 24px;
    background-color: ${ANTD_GRAY_V2[10]};
    display: flex;
    justify-content: flex-end;
    ${(props) =>
        !props.$hideBackground &&
        `
        background-image: url(${BackgroundDots});
        background-position: right;
        background-repeat: no-repeat;
    `}
`;

export const NavigationWrapper = styled.div<{ isHidden: boolean }>`
    font-size: 20px;
    color: white;
    display: flex;
    flex-wrap: nowrap;
    ${(props) => props.isHidden && 'opacity: 0;'}
`;

export const ArrowLeft = styled(ArrowLeftOutlined)`
    margin-right: 24px;
    cursor: pointer;
`;

export const ArrowRight = styled(ArrowRightOutlined)`
    margin-left: 24px;
    cursor: pointer;
`;
