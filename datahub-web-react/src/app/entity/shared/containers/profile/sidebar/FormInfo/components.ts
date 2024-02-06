import { ReadFilled, ReadOutlined } from '@ant-design/icons';
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
`;

export const Title = styled.div`
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 4px;
`;

export const StyledDivider = styled(Divider)`
    margin: 12px 0 0 0;
`;

export const StyledReadOutlined = styled(ReadOutlined)<{ addLineHeight?: boolean }>`
    margin-right: 8px;
    height: 13.72px;
    width: 17.5px;
    color: #373d44;
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;

export const StyledReadFilled = styled(ReadFilled)<{ addLineHeight?: boolean }>`
    margin-right: 8px;
    height: 13.72px;
    width: 17.5px;
    color: #7532a4;
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;

export const CTAWrapper = styled.div<{ shouldDisplayBackground?: boolean }>`
    color: #373d44;
    font-size: 14px;
    ${(props) =>
        props.shouldDisplayBackground &&
        `
        border-radius: 8px;
        padding: 16px;
        background-color: #f9f0ff;
        border: 1px solid #8338b8;
        `}
`;
