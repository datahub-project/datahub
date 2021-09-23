import Cookies from 'js-cookie';
import { v4 as uuidv4 } from 'uuid';
import { BROWSER_ID_COOKIE } from '../conf/Global';

function generateBrowserId(): string {
    return uuidv4();
}

export function getBrowserId() {
    let browserId = Cookies.get(BROWSER_ID_COOKIE);
    if (!browserId) {
        browserId = generateBrowserId();
        Cookies.set(BROWSER_ID_COOKIE, browserId, { expires: 365 });
    }
    return browserId;
}
