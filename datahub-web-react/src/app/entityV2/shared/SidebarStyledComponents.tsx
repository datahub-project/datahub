import styled from 'styled-components';
import { Row } from 'antd';
import DraftsOutlinedIcon from '@mui/icons-material/DraftsOutlined';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from './constants';

/**
 * Styled Components- Users and Groups Side bar component
 * Description: The following styles are used for User and Groups UI for sidebar.
 */

export const SideBar = styled.div`
    padding: 0 17px;
    text-align: center;
    font-style: normal;
    font-weight: bold;
    position: relative;

    &&& .ant-avatar.ant-avatar-icon {
        font-size: 46px !important;
    }

    .divider-infoSection {
        margin: 18px 0px 18px 0;
    }
    .divider-aboutSection {
        margin: 23px 0px 11px 0;
    }
    .divider-groupsSection {
        margin: 23px 0px 11px 0;
    }
`;

export const SideBarSubSection = styled.div`
    height: calc(100vh - 135px);
    overflow: auto;
    padding-right: 18px;
    &.fullView {
        height: calc(100vh - 70px);
    }
    &::-webkit-scrollbar {
        height: 12px;
        width: 1px;
        background: #d6d6d6;
    }
    &::-webkit-scrollbar-thumb {
        background: #d6d6d6;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

export const EmptyValue = styled.div<{ color?: string }>`
    &:after {
        content: 'None';
        color: ${(props) => (props.color ? props.color : '#b7b7b7')};
        font-style: italic;
        font-weight: 100;
    }
`;

export const Name = styled.div`
    font-size: 12px;
    line-height: 18px;
    color: ${ANTD_GRAY_V2['11']};
    text-align: left;
    display: flex;
    align-items: center;
    gap: 0.2rem;
    span {
        text-overflow: ellipsis;
        overflow: hidden;
        white-space: nowrap;
    }
    @media only screen and (min-width: 1200px) {
        color: ${REDESIGN_COLORS.WHITE};
    }
`;

export const TitleRole = styled.div`
    font-size: 12px;
    line-height: 18px;
    color: ${ANTD_GRAY_V2['11']};
    text-align: left;
    white-space: wrap;
    @media only screen and (min-width: 1200px) {
        color: ${REDESIGN_COLORS.WHITE};
    }
`;

export const RoleName = styled.div`
    text-align: center;
    color: ${REDESIGN_COLORS.WHITE};
    border-radius: 30px;
    background-color: #565657;
    padding: 3px 5px;
    text-transform: uppercase;
    font-size: 7px;
    line-height: 9px;
    position: relative;
    z-index: 2;
    @media only screen and (min-width: 1200px) {
        background-color: ${ANTD_GRAY[1]}4a;
    }
`;

export const Team = styled.div`
    font-size: 12px;
    line-height: 20px;
    color: #8c8c8c;
`;

export const SocialDetails = styled.div`
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 12px;
    line-height: 20px;
    color: ${ANTD_GRAY_V2['11']};
    text-align: left;
    span {
        text-overflow: ellipsis;
        overflow: hidden;
        white-space: nowrap;
    }
    .ant-space-item {
        display: flex;
        align-items: center;
    }
`;

export const EditButton = styled.div`
    bottom: 24px;
    position: absolute;
    right: 27px;
    width: 80%;
    left: 50%;
    -webkit-transform: translateX(-50%);
    -moz-transform: translateX(-50%);
    transform: translateX(-50%);

    button {
        width: 100%;
        font-size: 12px;
        line-height: 20px;
        color: #262626;
    }
`;

export const AboutSection = styled.div`
    text-align: left;
    font-weight: bold;
    font-size: 14px;
    line-height: 22px;
    color: #262626;
`;

export const AboutSectionText = styled.div`
    font-size: 12px;
    font-weight: 100;
    line-height: 15px;
    color: #434863;

    &&& .ant-typography {
        margin-bottom: 0;
    }
    &&& .ant-typography-edit-content {
        padding-left: 15px;
        padding-top: 5px;
    }
`;

export const GroupsSection = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
    text-align: left;
`;

export const TagsSection = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    margin-top: 0.5rem;
    align-self: start;
`;

export const NoDataFound = styled.span`
    font-size: 12px;
    color: #262626;
    font-weight: 100;
`;

export const Tags = styled.div`
    margin-top: 5px;
`;

export const GroupsSeeMoreText = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #1890ff;
    cursor: pointer;
`;

export const DisplayCount = styled.span`
    font-family: Mulish;
    font-style: normal;
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #8c8c8c;
`;

export const GroupSectionTitle = styled.span`
    margin-right: 8px;
`;

export const GroupSectionHeader = styled.div`
    padding-bottom: 12px;
`;

export const Content = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;

    & > div {
        padding-top: 12px;
        padding-bottom: 12px;
        width: 100%;
        &:not(:last-child) {
            border-bottom: 1px dashed;
            border-color: rgba(0, 0, 0, 0.3);
        }
    }
`;

export const CustomAvatarContainer = styled.div`
    position: relative;
    margin-top 20px;
    &:hover{
        .edit-button-container{
            display: block;
        }
    }
`;

export const UserInfo = styled(Row)`
    display: flex;
    gap: 1rem;
    padding: 10px 10px;
    background: ${ANTD_GRAY_V2['14']};
    border-radius: 10px;
    justify-content: center;
    @media only screen and (min-width: 1200px) {
        justify-content: flex-start;
    }
    @media only screen and (min-width: 1600px) {
        padding: 20px 20px;
    }
`;

export const GroupInfo = styled.div`
    display: flex;
    flex-direction: column;
    flex-wrap: wrap;
    background: ${ANTD_GRAY_V2['14']};
    border-radius: 10px;
    overflow: hidden;
`;

export const SocialInfo = styled.div`
    display: flex;
    flex-direction: column;
    z-index: 2;
    position: relative;
    justify-content: flex-end;
    gap: 0.4rem;
`;

export const GradientContainer = styled.div<{ gradient?: string; height?: number }>`
    border-radius: 10px 10px 0px 0px;
    background: ${(props) => (props.gradient ? props.gradient : REDESIGN_COLORS.PROFILE_AVATAR_STYLE_GRADIENT)};
    height: 65px;
    width: 100%;
    z-index: 1;
    display: flex;
    gap: 10px;
    justify-content: start;
    padding: 0 10px;
    position: absolute;
    @media only screen and (min-width: 1200px) and (max-width: 1600px) {
        height: 55px;
    }
`;

export const BasicDetailsContainer = styled.div`
    font-size: 12px;
    align-items: start;
    position: relative;
`;

export const EditProfileButtonContainer = styled.div`
    position: absolute;
    top: 10px;
    right: 10px;
    z-index: 2;
    cursor: pointer;
    display: none;
    &&& div {
        border-color: ${REDESIGN_COLORS.WHITE};
        height: 18px;
        width: 18px;
        display: flex;
        align-items: center;
        &:hover {
            border-color: ${REDESIGN_COLORS.BORDER_4};
        }
    }
`;

export const BasicDetails = styled.div`
    position: relative;
    z-index: 2;
    display: flex;
    flex-direction: column;
    .ant-divider {
        margin: 0.5rem 0px;
    }
    @media only screen and (min-width: 1200px) {
        gap: 1rem;
        .ant-divider {
            display: none;
        }
    }
`;

export const DraftsOutlinedIconStyle = styled(DraftsOutlinedIcon)`
    font-size: 12px !important;
`;

export const SubscriptionContainer = styled(Row)`
    display: flex;
    gap: 0.5rem;
`;

export const OwnershipContainer = styled(Row)`
    display: flex;
    gap: 0.5rem;
`;

export const DisplayNameText = styled.span`
    color: ${ANTD_GRAY_V2[12]};
    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    font-weight: 600;
    line-height: normal;
`;

export const NameTitleContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export const WhiteEditOutlinedIconStyle = styled(EditOutlinedIcon)`
    color: ${REDESIGN_COLORS.WHITE};
    height: 18px !important;
    width: 18px !important;
    &:hover,
    &:focus {
        color: ${REDESIGN_COLORS.WHITE};
        border-color: ${REDESIGN_COLORS.WHITE};
    }
`;

export const ShowMoreButton = styled.div`
    margin-top: 8px;
    padding: 0px;
    color: ${ANTD_GRAY[7]};
    text-align: left;
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

export const CountStyle = styled.div`
    background-color: #ebecf0;
    border-radius: 100px;
    color: #5b6282;
    padding: 0px 10px;
    margin: 0px 5px;
    font-size: 10px;
    line-height: 18px;
`;
