import { colors } from '@components';
import styled from 'styled-components/macro';

export const ModalSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export const SectionTitle = styled.div`
    color: ${colors.gray[600]};
    font-family: var(--semantics-label-font-family, Mulish);
    font-size: 12px;
    font-weight: 700;
    margin-bottom: 4px;
    display: block;
`;

export const InputRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: flex-start;
`;

export const InputContainer = styled.div<{ $hasError?: boolean }>`
    flex: 1;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 8px;
    background: ${colors.white};
    border: 1px solid ${(props) => (props.$hasError ? colors.red[500] : colors.gray[100])};
    border-radius: 6px;
    box-shadow: 0px 4px 8px 0px rgba(33, 23, 95, 0.04);
    height: 32px;
    position: relative;
`;

export const OrDivider = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;

    &::before,
    &::after {
        content: '';
        flex: 1;
        height: 1px;
        background-color: ${colors.gray[100]};
    }

    span {
        color: ${colors.gray[1700]};
        font-size: 12px;
        font-weight: 700;
    }
`;

export const InvitedUsersSection = styled.div`
    margin-top: 1px;
`;

export const InviteUsersTabsSection = styled.div`
    margin-bottom: 4px;
`;

export const InvitedUsersLabel = styled(SectionTitle)`
    color: ${colors.gray[1700]};
`;

export const InvitedUsersList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 0;
`;

export const InvitedUserItem = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 4px;
    border-radius: 4px;
    transition: background-color 0.2s;

    &:hover {
        background-color: ${colors.gray[1500]};
    }
`;

export const UserEmail = styled.span`
    flex: 1;
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const UserStatus = styled.span`
    font-size: 12px;
    font-weight: 600;
    color: ${colors.gray[1700]};
    padding: 2px 6px;
    border-radius: 4px;
    background-color: ${colors.gray[1500]};
    border: 1px solid ${colors.gray[100]};
    min-width: 60px;
    text-align: center;
`;

export const ValidationErrorText = styled.div`
    color: ${colors.red[500]};
    font-size: 12px;
    font-weight: 400;
    margin-top: 4px;
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const EmailInputSection = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 24px;
    margin-bottom: 4px;
`;

export const WarningIconContainer = styled.div`
    color: ${colors.red[500]};
    display: flex;
    align-items: center;
    margin-left: auto;
`;

export const ErrorMessageOverlay = styled.div`
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    color: ${colors.red[1000]};
    font-size: 12px;
    font-weight: 400;
    margin-top: 4px;
    display: flex;
    align-items: center;
    gap: 4px;
    z-index: 10;
`;

export const InputRowControls = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

export const EmailErrorMessage = styled.div<{ $visible: boolean }>`
    color: ${colors.red[1000]};
    font-size: 12px;
    font-weight: 400;
    min-height: ${(props) => (props.$visible ? 'auto' : '16px')};
    display: ${(props) => (props.$visible ? 'block' : 'none')};
`;
