import React, { useContext, useEffect } from 'react';
import { LoadingOutlined } from '@ant-design/icons';
import { Body, StyledSpin } from './styledComponents';
import FormHeader from './FormHeader';
import FormContent from './FormContent';
import { FormMode } from './formUtils';
import ManageFormContext from './ManageFormContext';

interface Props {
    mode: FormMode;
}

const FormBody = ({ mode }: Props) => {
    const { setFormMode, isFormLoading } = useContext(ManageFormContext);

    useEffect(() => {
        setFormMode(mode);
    }, [mode, setFormMode]);

    return (
        <Body>
            <StyledSpin spinning={isFormLoading} indicator={<LoadingOutlined />}>
                <FormHeader />
                <FormContent />
            </StyledSpin>
        </Body>
    );
};

export default FormBody;
