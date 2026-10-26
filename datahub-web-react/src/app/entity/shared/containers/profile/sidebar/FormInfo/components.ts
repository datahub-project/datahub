import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import { SealCheck } from '@phosphor-icons/react/dist/csr/SealCheck';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { Divider } from 'antd';
import styled from 'styled-components';

export const FlexWrapper = styled.div`
    display: flex;
    line-height: 18px;
`;

export const StyledSealCheck = styled(SealCheck).attrs({ size: 18, weight: 'fill' })<{ addLineHeight?: boolean }>`
    margin-right: 8px;
    flex-shrink: 0;
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;

export const GreenSealCheck = styled(StyledSealCheck)`
    color: ${(props) => props.theme.colors.iconSuccess};
`;

export const PurpleSealCheck = styled(StyledSealCheck)`
    color: ${(props) => props.theme.colors.iconBrand};
`;

export const GrayWarningCircle = styled(WarningCircle).attrs({ size: 18, weight: 'fill' })`
    margin-right: 8px;
    flex-shrink: 0;
    color: ${(props) => props.theme.colors.icon};
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
    margin-bottom: 4px;
`;

export const StyledDivider = styled(Divider)`
    margin: 12px 0 0 0;
`;

export const StyledReadOutlined = styled(BookOpen)<{ addLineHeight?: boolean }>`
    margin-right: 8px;
    height: 13.72px;
    width: 17.5px;
    color: ${(props) => props.theme.colors.text};
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;

export const StyledReadFilled = styled(BookOpen).attrs({ weight: 'fill' })<{ addLineHeight?: boolean }>`
    margin-right: 8px;
    height: 13.72px;
    width: 17.5px;
    color: ${(props) => props.theme.colors.iconBrand};
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;

export const CTAWrapper = styled.div<{ shouldDisplayBackground?: boolean }>`
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    ${(props) =>
        props.shouldDisplayBackground &&
        `
        border-radius: 8px;
        padding: 16px;
        background-color: ${props.theme.colors.bgSurfaceBrand};
        border: 1px solid ${props.theme.colors.borderBrand};
        `}
`;
