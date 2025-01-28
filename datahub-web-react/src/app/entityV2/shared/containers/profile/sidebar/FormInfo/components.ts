import { ReadFilled, ReadOutlined } from '@ant-design/icons';
import KeyboardArrowDownOutlinedIcon from '@mui/icons-material/KeyboardArrowDownOutlined';
import Icon from '@ant-design/icons/lib/components/Icon';
import { Divider } from 'antd';
import styled from 'styled-components';

export const FlexWrapper = styled.div`
    display: flex;
    line-height: 18px;
`;

export const StyledIcon = styled(Icon)<{ addLineHeight?: boolean }>`
    font-size: 18px;
    margin-right: 8px;
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;

export const SubTitle = styled.div<{ addMargin?: boolean }>`
    font-weight: 600;
    margin-bottom: 4px;
    ${(props) => props.addMargin && `margin-top: 8px;`}
    text-wrap: wrap;
`;

export const Title = styled.div`
    font-size: 16px;
    font-weight: 600;
    display: flex;
    align-items: center;
`;

export const StyledDivider = styled(Divider)`
    margin: 12px 0 0 0;
`;

export const StyledReadOutlined = styled(ReadOutlined)<{ color?: string; addLineHeight?: boolean }>`
    margin-right: 8px;
    height: 18px;
    width: 18px;
    color: #373d44;
    ${(props) => props.addLineHeight && `line-height: 24px;`}
    ${(props) => props.color && `color: ${props.color};`}
`;

export const StyledReadFilled = styled(ReadFilled)<{ color: string; addLineHeight?: boolean }>`
    margin-right: 8px;
    height: 18px;
    width: 18px;
    color: #7532a4;
    ${(props) => props.addLineHeight && `line-height: 24px;`}
    ${(props) => props.color && `color: ${props.color};`}
`;

export const CTAWrapper = styled.div<{ backgroundColor?: string; borderColor?: string; padding?: string }>`
    color: #373d44;
    font-size: 14px;
    min-width: 180px;
    ${(props) =>
        `
        border-radius: 8px;
        padding: ${props.padding || '16px'};
        background-color: ${props.backgroundColor ? props.backgroundColor : '#f9f0ff'};
        border: 1px solid ${props.borderColor ? props.borderColor : '#8338b8'};
        `}
`;

export const Content = styled.div`
    width: 100%;
`;

export const TitleWrapper = styled.div<{ isOpen?: boolean; isUserAssigned?: boolean }>`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    margin-bottom: ${(props) => (props.isOpen ? '10px' : '0px')};
    cursor: ${(props) => (props.isUserAssigned ? 'pointer' : 'not-allowed')};
    text-wrap: wrap;
`;

export const StyledArrow = styled(KeyboardArrowDownOutlinedIcon)<{ isOpen: boolean }>`
    font-size: 12px;
    margin-left: 3px;
    cursor: pointer;
    ${(props) =>
        props.isOpen &&
        `
        transform: rotate(180deg);
        padding-top: 1px;
    `}
`;

export const StyledButtonWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export const StyledImgIcon = styled.img<{ addLineHeight?: boolean; disable?: boolean }>`
    font-size: 24px;
    margin-right: 8px;
    align-self: flex-start;
    ${(props) => props.disable && `opacity: 0.5;`};
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;
