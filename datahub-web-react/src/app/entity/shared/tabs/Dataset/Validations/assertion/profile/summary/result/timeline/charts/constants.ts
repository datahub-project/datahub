import { Maybe } from "graphql/jsutils/Maybe";
import { AssertionInfo, AssertionType, FieldAssertionType } from "../../../../../../../../../../../../types.generated";

export enum AssertionChartType {
    ValuesOverTime,
    StatusOverTime,
    // TODO: special case for freshness (for now it'll be status over time)
}
export const VALUES_OVER_TIME_ASSERTION_TYPES: AssertionType[] = [AssertionType.Field, AssertionType.Sql, AssertionType.Volume]
export const getBestChartTypeForAssertion = (assertionInfo?: AssertionInfo | Maybe<AssertionInfo>): AssertionChartType => {
    if (assertionInfo && VALUES_OVER_TIME_ASSERTION_TYPES.includes(assertionInfo.type)) {
        switch (assertionInfo.type) {
            case AssertionType.Field:
                return AssertionChartType.ValuesOverTime;
            case AssertionType.Sql:
                break;
            case AssertionType.Volume:
                return AssertionChartType.ValuesOverTime;
            default:
                break;
        }
    }
    return AssertionChartType.StatusOverTime; // safest catch-all fallback
} 