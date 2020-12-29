package utils;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;

import java.util.List;
import java.util.Map;

public class GraphQLUtil {

    private GraphQLUtil() { }

    /**
     * Convert a Map<String, Object> to a Rest.li DataMap by recursively
     * building DataLists & DataMaps.
     *
     * This method assumes that all map keys are instances of String
     * and all values are either Maps, Lists, or java primitives.
     */
    public static DataMap toDataMap(Map<String, Object> map) {
        DataMap dataMap = new DataMap();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, Object> childMap = (Map<String, Object>) entry.getValue();
                dataMap.put(entry.getKey(), toDataMap(childMap));
            } else if (entry.getValue() instanceof List) {
                List<Object> childList = (List<Object>) entry.getValue();
                dataMap.put(entry.getKey(), toDataList(childList));
            } else {
                dataMap.put(entry.getKey(), entry.getValue());
            }
        }
        return dataMap;
    }

    /**
     * Convert a List<Object> to a Rest.li DataList by recursively
     * building DataLists & DataMaps.
     *
     * This method assumes that all map keys are instances of String
     * and all values are either Maps, Lists, or java primitives.
     */
    public static DataList toDataList(List<Object> list) {
        DataList dataList = new DataList();
        for (Object item : list) {
            if (item instanceof Map) {
                Map<String, Object> childMap = (Map<String, Object>) item;
                dataList.add(toDataMap(childMap));
            } else if (item instanceof List) {
                List<Object> childList = (List<Object>) item;
                dataList.add(toDataList(childList));
            } else {
                dataList.add(item);
            }
        }
        return dataList;
    }

}
