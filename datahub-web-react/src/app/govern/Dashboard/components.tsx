import { Select, Typography } from 'antd';
import styled from 'styled-components';

export const Layout = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    overflow: hidden;
    margin: ${(props) => (props.$isShowNavBarRedesign ? '5px' : '0 16px 12px 0')};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex: 1;
    flex-direction: column;
    background-color: ${(props) => props.theme.colors.bg};
    ${(props) => props.$isShowNavBarRedesign && `box-shadow: ${props.theme.colors.shadowSm};`}
`;

export const Header = styled.div`
    display: flex;
    min-height: 85px;
    align-items: center;
    justify-content: space-between;
    padding: 16px 20px 20px 20px;
`;

export const TabsContainer = styled.div<{ isThemeV2: boolean; formCreationEnabled: boolean }>`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 1rem;

    ${(props) =>
        !props.formCreationEnabled
            ? `
        
        height: 50px;
        border-bottom: 1px solid ${props.theme.colors.border};

        .ant-tabs {
            margin-bottom: -17px;
        }

        .ant-tabs-tab {
            font-size: 16px;
        }

    `
            : `

        align-items: center;
        height: 70px;

        .ant-tabs-nav {
            margin: 0;

            &:before {
                border-bottom: none !important;
            }
        }

        .ant-tabs-ink-bar {
            display: none;
        }

        .ant-tabs-tab {
            font-size: 14px;
            font-weight: 600;
            color: ${props.theme.colors.textTertiary};
            padding: 8px 16px !important;
            border-radius: 36px;
            border: 1px solid ${props.theme.colors.border};
            background-color: ${props.theme.colors.bg};
        }

        ${
            props.isThemeV2 &&
            `
                .ant-tabs-tab-active  {
                    background-color: ${props.theme.colors.buttonSurfaceSecondaryHover};
                    border: 1px solid ${props.theme.colors.buttonSurfaceSecondaryHover};
                }
                
                .ant-tabs-tab-active .ant-tabs-tab-btn {
                    color: ${props.theme.styles['primary-color']};
                }
            `
        }
    `}
`;

export const SeriesContainer = styled.div`
    display: flex;
    align-items: center;
    margin-right: 1rem;
`;

export const SeriesButtons = styled.div`
    display: flex;
    align-items: center;

    button {
        box-shadow: none;
        margin-left: -1px;
        font-weight: 400;
        font-size: 12px;

        &:first-child {
            border-top-right-radius: 0;
            border-bottom-right-radius: 0;
        }

        &:not(:first-child):not(:last-child) {
            border-radius: 0;
        }

        &:last-child {
            border-top-left-radius: 0;
            border-bottom-left-radius: 0;
        }
    }
`;

export const SeriesLabel = styled.div`
    font-size: 10px;
    margin-right: 0.5rem;
    opacity: 0.75;
`;

export const BodyHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
    height: 40px;

    svg {
        opacity: 0.25;

        &:hover {
            opacity: 0.75;
        }
    }
`;

export const DataFreshness = styled.div`
    span {
        display: flex;
        align-items: center;

        &:hover {
            cursor: default;
        }
    }

    svg {
        color: ${(props) => props.theme.colors.textWarning};
        margin-right: 0.15rem;
    }
`;

export const Filters = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

export const Body = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    display: flex;
    flex-direction: column;
    background-color: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.colors.bg : props.theme.colors.bgSurface};
    padding: 1rem;
`;

export const TabBody = styled.div`
    overflow: auto;
    height: calc(100% - 70px);
`;

export const ChartGroup = styled.div`
    width: 100%;
    margin-bottom: 2rem;
`;

export const Row = styled.div`
    display: flex;
    gap: 1rem;
    width: 100%;
`;

export const PrimaryHeading = styled(Typography.Text)`
    font-size: 24px;
    font-weight: 600;
`;

export const SecondaryHeading = styled(Typography.Text)`
    display: block;
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 0.5rem;
    color: inherit;
`;

export const StatusSeriesWrapper = styled.div`
    width: 100%;
`;

export const StatusSeriesHeading = styled(Typography.Text)`
    display: block;
    font-size: 18px;
    font-weight: 600;
    margin-top: 0.5rem;
    color: ${(props) => props.theme.colors.textSuccess};
`;

export const StatusSeriesDescription = styled(Typography.Text)`
    display: block;
    font-size: 12px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textTertiary};
    max-width: 85%;
`;

export const ChartPerformanceItems = styled.div`
    width: 100%;
`;

export const ChartPerformanceItem = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    margin-top: 1rem;
`;

export const StyledSelect = styled(Select)`
    .ant-select-arrow {
        color: ${(props) => props.theme.colors.text};
    }
`;
