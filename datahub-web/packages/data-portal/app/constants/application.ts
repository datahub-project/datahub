import moment from 'moment';

/**
 * Defines the props for the feedback mailto: link
 */
// TODO META-9767: Move Provide Feedback link to configurator in midtier
const subject = `DataHub Feedback ${moment().format('MMMM Do YYYY, h:mm:ss a')}`;
export const feedback = { title: 'Provide Feedback', subject };
