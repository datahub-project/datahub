import { message } from 'antd';

export function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

export const printSuccessMsg = async (status) => {
    message.success(`Status:${status} - Request submitted successfully. Reloading Page..`, 3).then();
    // only reload page if the call is successful, else do not reload.
    await timeout(3000);
    window.location.reload();
};
export const printErrorMsg = (error) => {
    message.error(error, 10).then();
};
