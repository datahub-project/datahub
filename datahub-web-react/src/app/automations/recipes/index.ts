import snowflakeTagPropagation from './snowflakeTagPropagation';
import termPropagation from './termPropagation';
import columnLevelDocPropagation from './columnLevelDocPropagation';
import custom from './custom';

const recipes = {
    snowflakeTagPropagation,
    termPropagation,
    columnLevelDocPropagation,
    custom,
};

export default recipes;
