// Dayjs is missing core functionality without this. It causes issues in setting default value of antd datepicker without.
import dayjs from 'dayjs';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import localeData from 'dayjs/plugin/localeData';
import weekday from 'dayjs/plugin/weekday';
import weekOfYear from 'dayjs/plugin/weekOfYear';
import weekYear from 'dayjs/plugin/weekYear';

import * as Browse from './Browse';
import * as Global from './Global';
import * as Search from './Search';

dayjs.extend(customParseFormat);
dayjs.extend(advancedFormat);
dayjs.extend(weekday);
dayjs.extend(localeData);
dayjs.extend(weekOfYear);
dayjs.extend(weekYear);

// TODO: A way to populate configs without code changes?
// TOOD: Entity-oriented configurations?
export { Browse as BrowseCfg, Global as GlobalCfg, Search as SearchCfg };
