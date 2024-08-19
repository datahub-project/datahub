import { Result } from 'antd';
import { useTranslation } from 'react-i18next';
export const UnauthorizedPage = () => {
    const { t } = useTranslation();
    return (
        <>
            <Result status="403" title="Unauthorized" subTitle={t('crud.error.notAuthorizedToAccessPage')} />
        </>
    );
};
