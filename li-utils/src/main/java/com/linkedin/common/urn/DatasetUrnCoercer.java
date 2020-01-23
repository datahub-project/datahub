package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;

public class DatasetUrnCoercer implements DirectCoercer<DatasetUrn> {

    private static final boolean REGISTER_COERCER = Custom.registerCoercer(new DatasetUrnCoercer(), DatasetUrn.class);

    public DatasetUrnCoercer() {
    }

    public Object coerceInput(DatasetUrn object) throws ClassCastException {
        return object.toString();
    }

    public DatasetUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
            return DatasetUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
            throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
    }
}
