import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import { sharedStyles } from '../sharedComponents';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const sidebarWidth = '250px';

// Page Container

export const AutomationsPageContainer = styled.div`
    max-width: 98%;
    min-height: 98%;
    overflow: hidden;
    // TODO: Readd for sidebar
    // display: grid;
    // grid-template-columns: ${sidebarWidth} 1fr;
    // grid-template-rows: 1fr;
    gap: 16px;
    z-index: 100;

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

export const AutomationsContentHeader = styled.div`
    position: sticky;
    top: 0;
    z-index: 70;
    background-color: #fff;
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 30px;
    border-bottom: 1px solid ${sharedStyles.borderColor};

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

export const AutomationsContentBody = styled.div`
    padding: 16px 30px;
`;

export const AutomationsBody = styled.div`
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
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
		color: ${sharedStyles.activeColor};
		border-color: ${sharedStyles.activeColor};
	`}

    &:hover {
        cursor: pointer;
    }
`;

// Automations List Card

export const ListCard = styled.div`
    padding: 24px;
    border-radius: 12px;
    border: 1px solid ${sharedStyles.darkBorderColor};

    &:hover {
        border-color: ${sharedStyles.activeColor};
        cursor: pointer;
    }
`;

export const ListCardHeader = styled.div`
    & .categoryAndDeployed {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 4px;

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
            font-size: 14px;
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
            font-size: 22px;
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
        padding: 4px 8px;
        border-radius: 20px;
        background: ${sharedStyles.statusInactiveColor};
        color: ${sharedStyles.statusInactiveFontColor};
    }
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

export const PremadeAutomationCard = styled.div<{ isDisabled: boolean }>`
    padding: 16px;
    border-radius: 8px;
    border: 1px solid ${sharedStyles.borderColor};

    &:hover {
        border-color: ${sharedStyles.activeColor};
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

    ${({ isDisabled }) =>
		isDisabled &&
		`
		opacity: 0.5;
		pointer-events: none;
		cursor: not-allowed;

		&:hover {
			border-color: ${sharedStyles.borderColor};
			cursor: not-allowed;
		}
	`}
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
`;

export const AutomationLogo = styled.img`
    width: 35px;
    height: 35px;
    object-fit: contain;
    margin-bottom: 16px;
`;

export const YamlButtonsContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 16px;
`;

export const ButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

export const IconContainer = styled.div<{ addExtraPadding?: boolean; disabled?: boolean }>`
    border-radius: 50%;
    color: ${sharedStyles.activeColor};
    border: 2px solid ${sharedStyles.buttonBorderColor};
    padding: 3px;
    display: flex;
    align-items: center;
    justify-content: center;
    height: 30px;
    width: 30px;

    svg {
        height: 18px;
        width: 18px;
    }
`;

export const UndoButton = styled.div`
    border-radius: 50%;
    color: ${sharedStyles.activeColor};
    border: 2px solid ${sharedStyles.buttonBorderColor};
    padding: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    height: 30px;
    width: 30px;

    svg {
        height: 18px;
        width: 18px;

        path {
            fill: ${sharedStyles.activeColor};
        }
    }
`;

export const StyledDivider = styled(Divider)`
    margin: 14px 0;
    border-top: 1px solid ${sharedStyles.dividerColor};
`;

export const Category = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 900;
    color: ${sharedStyles.greyDisabled};
    letter-spacing: 2.4px;
`;

export const ContentTitle = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
    color: ${sharedStyles.headingColor};
`;

export const Details = styled.div`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;