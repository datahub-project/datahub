package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;

public class DataPlatformUrnCoercer implements DirectCoercer<DataPlatformUrn> {

    private static final boolean REGISTER_COERCER = Custom.registerCoercer(new DataPlatformUrnCoercer(), DataPlatformUrn.class);

    public DataPlatformUrnCoercer() {
    }

    public Object coerceInput(DataPlatformUrn object) throws ClassCastException {
        return object.toString();
    }

    public DataPlatformUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
            return DataPlatformUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
            throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
    }
}
