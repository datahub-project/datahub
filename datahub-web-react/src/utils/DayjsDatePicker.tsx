import generatePicker from 'antd/es/date-picker/generatePicker';
import type { Dayjs } from 'dayjs';
import dayjsGenerateConfig from 'rc-picker/es/generate/dayjs';

const DatePicker = generatePicker<Dayjs>(dayjsGenerateConfig);

export default DatePicker;
