import { message } from 'antd';

export function timeout(delay: number) {
    return new Promise((res) => setTimeout(res, delay));
}

export const printSuccessMsg = (status) => {
    message.success(`Status:${status} - Request submitted successfully. Reloading Page..`, 3).then();
};
export const printErrorMsg = (error) => {
    message.error(error, 10).then();
};
