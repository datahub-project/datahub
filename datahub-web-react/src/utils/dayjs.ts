import dayjs from 'dayjs';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import duration from 'dayjs/plugin/duration';
import isoWeek from 'dayjs/plugin/isoWeek';
import localeData from 'dayjs/plugin/localeData';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import relativeTime from 'dayjs/plugin/relativeTime';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import weekOfYear from 'dayjs/plugin/weekOfYear';
import weekYear from 'dayjs/plugin/weekYear';
import weekday from 'dayjs/plugin/weekday';

dayjs.extend(advancedFormat);
dayjs.extend(customParseFormat);
dayjs.extend(duration);
dayjs.extend(isoWeek);
dayjs.extend(localeData);
dayjs.extend(localizedFormat);
dayjs.extend(relativeTime);
dayjs.extend(timezone);
dayjs.extend(utc);
dayjs.extend(weekOfYear);
dayjs.extend(weekYear);
dayjs.extend(weekday);

// dayjs ships English built in; other locales are registered by a side-effecting import. Splitting
// them into dynamic imports keeps non-English locale data out of the main chunk — each is fetched
// only when that language is selected. Keep keys in sync with `LOCALE_MAP` in `app/i18n/constants`.
const DAYJS_LOCALE_LOADERS: Record<string, () => Promise<unknown>> = {
    de: () => import('dayjs/locale/de'),
    es: () => import('dayjs/locale/es'),
    'pt-br': () => import('dayjs/locale/pt-br'),
};

/**
 * Loads the locale's data chunk (if any) and then activates it. `en` is built in, so it has no
 * loader and resolves immediately. Always `await` before relying on locale-dependent formatting.
 */
export async function setDayjsLocale(code: string): Promise<void> {
    await DAYJS_LOCALE_LOADERS[code]?.();
    dayjs.locale(code);
}

export default dayjs;
export type { Dayjs, ManipulateType } from 'dayjs';
