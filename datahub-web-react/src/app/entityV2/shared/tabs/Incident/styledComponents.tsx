import { Form } from 'antd';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';
import radius from '@src/alchemy-components/theme/foundations/radius';
import spacing from '@src/alchemy-components/theme/foundations/spacing';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

export const StyledTableContainer = styled.div`
    table tr.acryl-selected-table-row {
        background-color: ${ANTD_GRAY[4]};
    }
    margin: 0px 12px 12px 12px;
`;

export const LinkedAssetsContainer = styled.div<{ hasButton?: boolean; width?: string }>(({ hasButton }) => ({
    border: `1px solid ${colors.gray[100]}`,
    borderRadius: radius.lg,
    padding: spacing.xxsm,
    boxShadow: '0px 1px 2px 0px rgba(33, 23, 95, 0.07)',
    backgroundColor: colors.white,
    width: 'auto',
    maxHeight: '40vh',
    overflow: 'auto',

    '&:hover': hasButton
        ? {
              border: `1px solid ${colors.violet[500]}`,
              cursor: 'pointer',
          }
        : {},
}));

export const FiltersContainer = styled.div`
    display: flex;
    gap: 16px;
`;

export const StyledFilterContainer = styled.div`
    button {
        box-shadow: none !important;
        height: 36px !important;
        font-size: 14px !important;
        border-radius: 8px !important;
        color: #5f6685;
    }
`;
export const SearchFilterContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 0px 10px;
    margin-bottom: 8px;
    margin-top: 8px;
    gap: 12px;
`;

export const ModalTitleContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export const ModalHeading = styled.span`
    font-weight: 700;
    font-size: 16px;
    color: ${colors.gray[600]};
`;

export const ModalDescription = styled.p`
    font-weight: 500;
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

export const FormItem = styled(Form.Item)`
    .ant-form-item-label > label {
        color: ${colors.gray[600]} !important;
        font-size: 12px;
    }
    .ant-form-item-control textarea {
        font-weight: 400;
        font-size: 14px;
    }
`;

export const IconContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 50%;
    border: 1px solid #ebecf0;
    height: 22px;
    width: 22px;
`;

export const ResolverNameContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: end;
    gap: 4px;
`;

export const ResolverInfoContainer = styled.div`
    display: flex;
    align-items: flex-start;
    flex-direction: column;
    gap: 8px;
    color: ${colors.gray[600]};
`;

export const ResolverTitleContainer = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
`;

export const ResolverDetailsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

export const ResolverSubTitleContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;
export const ResolverSubTitle = styled.div`
    font-size: 12px;
    font-weight: 700;
    color: ${colors.gray[600]};
`;

export const ResolverDetails = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1700]};
    width: 250px;
`;

export const AssigneeAvatarStackContainer = styled.div`
    display: flex;
`;

export const CategoryType = styled.div`
    color: ${REDESIGN_COLORS.BODY_TEXT};
    text-transform: capitalize;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100px;
`;
