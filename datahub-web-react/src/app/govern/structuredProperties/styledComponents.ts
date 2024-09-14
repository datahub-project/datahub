import { colors, typography } from '@src/alchemy-components';
import { Input, Typography } from 'antd';
import styled from 'styled-components';

export const PageContainer = styled.div`
    overflow: auto;
    margin: 0 12px 12px 0;
    padding: 16px;
    border-radius: 8px;
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 20px;
    background-color: ${colors.white};
`;

export const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const TableContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

export const HeaderContent = styled.div`
    display: flex;
    flex-direction: column;
`;

export const ButtonContainer = styled.div`
    display: flex;
    align-self: center;
`;

export const DataContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: calc(100% - 44px);
`;

export const PropName = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    line-height: normal;
`;

export const PropDescription = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1600]};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    line-height: normal;
`;

export const NameColumn = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

export const IconContainer = styled.div`
    height: 32px;
    width: 32px;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 12px;
    border-radius: 200px;
    background-color: ${colors.gray[1000]};
`;

export const PillsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const PillContainer = styled.div`
    display: flex;
`;

export const MenuItem = styled.div`
    display: flex;
    padding: 5px 100px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
`;

export const StyledSearch = styled(Input.Search)`
    height: 40px;
    width: 272px;

    .ant-input-wrapper {
        .ant-input-affix-wrapper {
            height: 40px;
            border-color: ${colors.gray[1400]};
            box-shadow: none;
            border-right: none;

            &:hover,
            &:focus {
                border-color: ${colors.gray[1400]};
            }

            input {
                color: ${colors.gray[600]};
            }
        }

        button {
            height: 40px;
            width: 40px;
            border-color: ${colors.gray[1400]};
            border-left: none;
            box-shadow: none;

            &:hover {
                border-color: ${colors.gray[1400]};
            }
        }
    }
`;
