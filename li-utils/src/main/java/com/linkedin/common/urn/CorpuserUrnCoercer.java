package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public class CorpuserUrnCoercer implements DirectCoercer<CorpuserUrn> {

    private static final boolean REGISTER_COERCER = Custom.registerCoercer(new CorpuserUrnCoercer(), CorpuserUrn.class);

    public CorpuserUrnCoercer() {
    }

    public Object coerceInput(CorpuserUrn object) throws ClassCastException {
        return object.toString();
    }

    public CorpuserUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
            return CorpuserUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
            throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
    }
}
