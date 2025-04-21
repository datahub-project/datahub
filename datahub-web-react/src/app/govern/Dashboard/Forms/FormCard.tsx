import { Icon } from '@components';
import MoreVertOutlinedIcon from '@mui/icons-material/MoreVertOutlined';
import { Dropdown, Typography } from 'antd';
import { getColor } from '@src/alchemy-components/theme/utils';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

const FormName = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
`;

const FormDescription = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Thumbnail = styled.div`
    height: 140px;
    background-color: ${REDESIGN_COLORS.SILVER_GREY};
    border-radius: 6px 6px 0 0;
`;

const FormContent = styled.div`
    display: flex;
    flex-direction: column;
    margin: 16px;
`;

const StyledMoreIcon = styled(MoreVertOutlinedIcon)`
    display: flex;
    padding: 3px;

    :hover {
        color: ${(props) => getColor('primary', 500, props.theme)};
    }
`;

const OptionsDropdown = styled.div`
    height: 24px;
    width: 24px;
    border-radius: 20px;
    background-color: ${REDESIGN_COLORS.ICON_ON_DARK};
    position: absolute;
    right: 15px;
    top: 15px;
    display: none !important;
`;

const CardContainer = styled.div`
    display: flex;
    flex-direction: column;
    border-radius: 12px;
    padding: 8px;
    box-shadow: 0px 0px 3px 0px rgba(0, 0, 0, 0.25);
    border: 0.5px solid ${REDESIGN_COLORS.SILVER_GREY};
    position: relative;

    :hover {
        border: 1px solid ${(props) => getColor('primary', 500, props.theme)};

        ${OptionsDropdown} {
            display: block !important;
        }
    }
`;

const MenuItem = styled.div`
    display: flex;
    gap: 5px;
    padding: 2px 20px 2px 4px;
    color: ${REDESIGN_COLORS.BODY_TEXT_GREY};
`;

interface Props {
    formData: {
        urn: string;
        formInfo: {
            name: string;
            description: string;
        };
    };
}

const FormCard = ({ formData }: Props) => {
    const history = useHistory();

    const items = [
        {
            key: '0',
            label: (
                <MenuItem onClick={() => history.push(`/govern/dashboard/edit-form/${formData.urn}`)}>
                    <Icon icon="EditNote" /> Edit
                </MenuItem>
            ),
        },
        {
            key: '1',
            label: (
                <MenuItem>
                    <Icon icon="Delete" /> Delete
                </MenuItem>
            ),
        },
    ];
    return (
        <CardContainer>
            <OptionsDropdown>
                <Dropdown menu={{ items }}>
                    <StyledMoreIcon />
                </Dropdown>
            </OptionsDropdown>
            <Thumbnail />
            <FormContent>
                <FormName>{formData?.formInfo?.name}</FormName>
                <FormDescription>{formData?.formInfo?.description}</FormDescription>
            </FormContent>
        </CardContainer>
    );
};

export default FormCard;
