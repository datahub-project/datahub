import { Divider, Typography } from 'antd';
import styled from 'styled-components';

import { colors } from '@components/theme';

import { AutomationStatus } from '@app/automations/constants';
import { sharedStyles } from '@app/automations/sharedComponents';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const sidebarWidth = '250px';

// Page Container

export const AutomationsPageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        max-width: 99%;
        min-height: 98%;
        z-index: 100;
    `}
    overflow: hidden;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 100%;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        margin: 5px;
    `}
    // TODO: Readd for sidebar
    // display: grid;
    // grid-template-columns: ${sidebarWidth} 1fr;
    // grid-template-rows: 1fr;
    gap: 16px;

    font-family: ${sharedStyles.fontFamily};
`;

export const AutomationsSidebar = styled.div`
    background-color: #fff;
    height: 100%;
    overflow: auto;
    width: ${sidebarWidth};
    padding: 10px 20px;
    border-radius: ${sharedStyles.borderRadius};
    box-shadow: ${sharedStyles.boxShadow};
`;

export const AutomationsContent = styled.div`
    background-color: #fff;
    height: 100%;
    width: 100%;
    overflow: auto;
    border-radius: ${sharedStyles.borderRadius};
    box-shadow: ${sharedStyles.boxShadow};
`;

// Page Header

export const AutomationsContentHeader = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        position: sticky;
        top: 0;
        z-index: 70;
    `}
    background-color: #fff;
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 20px 20px 20px;

    & h1 {
        color: ${sharedStyles.headingColor};
        font-size: 22px;
        font-weight: 700;
        margin: 0;
    }

    & p {
        color: ${sharedStyles.contentColor};
        font-size: 16px;
        font-weight: 400;
        margin: 0;
    }
`;

// Page Body

export const ScrollableContent = styled.div`
    overflow-x: hidden;
    overflow-y: auto;
    height: 100%;
`;

export const AutomationsContentBody = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding: 0px 20px;
    ${(props) => props.$isShowNavBarRedesign && 'height: calc(100% - 150px);'}
`;

export const AutomationsBody = styled.div`
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 16px;
    padding: 16px 0;
`;

// Page Tabs

export const AutomationsContentTabs = styled.div`
    display: flex;
    align-items: center;
    border-bottom: 1px solid ${sharedStyles.borderColor};
`;

export const AutomationsContentTab = styled.div<{ isActive: boolean }>`
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 12px 20px;
    border-bottom: 2px solid transparent;
    color: ${sharedStyles.contentColor};
    font-size: 12px;
    font-weight: 700;
    line-height: normal;
    margin-bottom: -1px;

    ${({ isActive }) =>
        isActive &&
        `
		color: ${(props) => getColor('primary', 500, props.theme)};
		border-color: ${(props) => getColor('primary', 500, props.theme)};
	`}

    &:hover {
        cursor: pointer;
    }

    & span {
        display: flex;
        align-items: center;
        justify-content: center;
        background-color: ${sharedStyles.borderColor};
        width: 10px;
        height: 10px;
        padding: 9px;
        font-size: 9px;
        color: ${sharedStyles.contentColor};
        border-radius: 100%;
    }
`;

// Automations List Card

export const ListCard = styled.div`
    padding: 24px;
    border-radius: 12px;
    border: 1px solid ${sharedStyles.borderColor};
`;

export const ListCardHeader = styled.div<{ status?: string | undefined }>`
    display: flex;
    justify-content: space-between;
    align-items: start;

    & .titleColumn {
        display: flex;
        flex-direction: column;
        align-items: start;
        justify-content: start;
        margin-bottom: 4px;
        gap: 42px;

        & h4 {
            color: ${sharedStyles.contentColor};
            font-size: 14px;
            font-weight: 400;
            margin: 0;
        }
    }

    & .deployedAndStatus {
        display: flex;
        gap: 8px;
        align-items: center;
        margin-bottom: 4px;

        & h4 {
            color: ${sharedStyles.contentColor};
            font-size: 12px;
            font-weight: 400;
            margin: 0;
        }
    }

    & .titleAndButtons {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;

        & h2 {
            color: ${sharedStyles.subHeadingColor};
            font-size: 18px;
            font-weight: 700;
            line-height: normal;
            margin: 0;

            // Elipsis for long text
            max-width: 80%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
    }

    & .status {
        display: flex;
        align-items: center;
        padding: 4px 8px;
        border-radius: 20px;
        background: ${sharedStyles.statusInactiveColor};
        color: ${sharedStyles.statusInactiveFontColor};
    }

    ${({ status }) =>
        status === AutomationStatus.ACTIVE &&
        `
			.status {
				background: ${colors.green['100']};
				color: ${colors.green['500']};

				& svg {
					color: ${colors.green['500']};
				}
			}
		`}

    ${({ status }) =>
        (status === AutomationStatus.INACTIVE || !status) &&
        `
			.status {
				background: ${ANTD_GRAY[4]};
				color: ${ANTD_GRAY[8]};

				& svg {
					color: ${ANTD_GRAY[8]};
				}
			}
		`}
`;

export const ListCardBody = styled.div`
    color: ${sharedStyles.contentColor};
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: 20px;

    & .createdBy {
        display: flex;
        align-items: center;
        margin-bottom: 10px;

        & .ant-avatar {
            margin: 0 4px;
        }
    }

    & .description {
        margin-bottom: 16px;

        // Elipsis for long text
        max-height: 40px;
        overflow: hidden;
    }
`;

export const ListCardFooter = styled.div;

// Create Modal

export const PremadeAutomations = styled.div`
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
`;

export const PremadeAutomationCard = styled.div`
    padding: 16px;
    border-radius: 8px;
    border: 1px solid ${sharedStyles.borderColor};

    &:hover {
        border-color: ${(props) => getColor('primary', 500, props.theme)};
        cursor: pointer;
    }

    & h2 {
        color: ${sharedStyles.subHeadingColor};
        font-size: 16px;
        font-weight: 700;
        line-height: normal;
    }

    & p {
        color: ${sharedStyles.contentColor};
        font-size: 12px;
        font-weight: 500;
        line-height: 20px;
    }
`;

export const AutomationsModalHeader = styled.div`
    display: flex;
    align-items: center;
    font-family: ${sharedStyles.fontFamily};

    & h2 {
        color: ${sharedStyles.subHeadingColor};
        font-size: 20px;
        font-weight: 700;
        line-height: normal;
        margin: 0;
    }

    & p {
        font-weight: normal;
        margin: 0;
    }

    & img {
        margin-bottom: 0;
        margin-right: 16px;
    }
`;

export const AutomationModalFooter = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 4px;

    & button {
        font-family: ${sharedStyles.fontFamily};
    }
`;

export const AutomationsDescription = styled.div`
    color: ${ANTD_GRAY[8]};
    font-weight: normal;
`;

export const AutomationLogo = styled.img`
    width: 35px;
    height: 35px;
    object-fit: contain;
    margin-bottom: 16px;
`;

export const Description = styled.div`
    color: ${ANTD_GRAY[7]};
    font-weight: normal;
    font-size: 14px;
    margin-top: 8px;
`;

export const YamlButtonsContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 8px;

    & .ant-btn > .anticon + span,
    & .ant-btn > span + .anticon {
        margin-left: 0;
    }
`;

export const ButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

export const StyledDivider = styled(Divider)`
    margin: 14px 0;
    border-top: 1px solid ${sharedStyles.dividerColor};
`;

export const Name = styled.h2`
    margin: 0px;
    padding: 0px;
    font-weight: 700;
`;

export const Category = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 900;
    color: ${sharedStyles.greyDisabled};
    letter-spacing: 2.2px;
`;

export const Details = styled.div`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

/*
 * Results
 */

export const ResultContainer = styled.div`
    & .pass {
        color: ${sharedStyles.success};
    }

    & .fail {
        color: ${sharedStyles.fail};
    }
`;

export const ResultGroup = styled.div({
    marginBottom: '8px',

    '& strong': {
        fontWeight: 700,
    },
});

export const TitleColumn = styled.div({
    display: 'flex',
    flexDirection: 'column',
    gap: '6px',
});
