import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import styled from 'styled-components';

export const FlexWrapper = styled.div`
    display: flex;
    line-height: 18px;
`;

export const Title = styled.div`
    font-size: 16px;
    font-weight: 600;
    display: flex;
    align-items: center;
`;

export const StyledDivider = styled.hr`
    margin: 12px 0 0 0;
    border: none;
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

export const StyledBookIcon = styled(BookOpen).attrs({ size: 18 })<{ $addLineHeight?: boolean }>`
    margin-right: 8px;
    flex-shrink: 0;
    color: ${(props) => props.theme.colors.icon};
    ${(props) => props.$addLineHeight && `line-height: 24px;`}
`;

export const CTAWrapper = styled.div<{ backgroundColor?: string; borderColor?: string; padding?: string }>`
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    min-width: 180px;
    ${(props) =>
        `
        border-radius: 8px;
        padding: ${props.padding || '16px'};
        background-color: ${props.backgroundColor ? props.backgroundColor : props.theme.colors.bgSurfaceBrand};
        border: 1px solid ${props.borderColor ? props.borderColor : props.theme.colors.borderBrand};
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

export const StyledArrow = styled(CaretDown).attrs({ size: 16 })<{ $isOpen: boolean }>`
    margin-left: 3px;
    flex-shrink: 0;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};
    ${(props) => props.$isOpen && `transform: rotate(180deg);`}
`;

export const StyledButtonWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    padding-top: 12px;
`;

export const StyledImgIcon = styled.img<{ addLineHeight?: boolean; disable?: boolean }>`
    font-size: 24px;
    margin-right: 8px;
    align-self: flex-start;
    ${(props) => props.disable && `opacity: 0.5;`};
    ${(props) => props.addLineHeight && `line-height: 24px;`}
`;
