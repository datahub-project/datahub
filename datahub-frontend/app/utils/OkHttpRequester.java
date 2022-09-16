package utils;

import com.google.gson.JsonElement;
import exception.ErrorResponseException;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class OkHttpRequester {
    private final OkHttpClient okHttpClient = new OkHttpClient();
    public static final MediaType APPLICATION_JSON_TYPE = MediaType.get("application/json");


    public JsonElement callSync(Request request) throws IOException {
        return callSync(request, JsonElement.class);
    }

    public <T> T callSync(Request request, Class<T> clazz) throws IOException {
        Call call = okHttpClient.newCall(request);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new ErrorResponseException(response.code());
            }
            ResponseBody responseBody = response.body();
            if (responseBody != null) {
                String bodyString = responseBody.string();
                if (!StringUtils.isEmpty(bodyString)) {
                    return GsonUtils.parse(bodyString, clazz);
                }
            }
            return GsonUtils.parse(null, clazz);
        }
    }

    public Call getCall(Request request) {
        return okHttpClient.newCall(request);
    }
}
