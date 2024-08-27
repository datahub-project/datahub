import styled from 'styled-components';

/**
 * Styled Components- Users and Groups Side bar component
 * Description: The following styles are used for User and Groups UI for sidebar.
 */

export const SideBar = styled.div`
    padding: 0 0 0 17px;
    text-align: center;

    font-style: normal;
    font-weight: bold;
    height: calc(100vh - 60px);
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

export const EmptyValue = styled.div`
    &:after {
        content: 'None';
        color: #b7b7b7;
        font-style: italic;
        font-weight: 100;
    }
`;

export const Name = styled.div`
    font-size: 20px;
    line-height: 28px;
    color: #262626;
    margin: 13px 0 7px 0;
`;

export const TitleRole = styled.div`
    font-size: 14px;
    line-height: 22px;
    color: #595959;
    margin-bottom: 7px;
`;

export const Team = styled.div`
    font-size: 12px;
    line-height: 20px;
    color: #8c8c8c;
`;

export const SocialDetails = styled.div`
    font-size: 12px;
    line-height: 20px;
    color: #262626;
    text-align: left;
    margin: 6px 0;
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
    padding: 5px 0;

    &&& .ant-typography {
        margin-bottom: 0;
    }
    &&& .ant-typography-edit-content {
        padding-left: 15px;
        padding-top: 5px;
    }
`;

export const GroupsSection = styled.div`
    text-align: left;
    font-weight: bold;
    font-size: 14px;
    line-height: 22px;
    color: #262626;
`;

export const TagsSection = styled.div`
    height: calc(75vh - 460px);
    padding: 0px 5px 5px 0;
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
    font-family: Manrope;
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
