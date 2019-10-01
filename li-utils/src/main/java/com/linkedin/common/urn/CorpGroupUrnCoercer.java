package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public class CorpGroupUrnCoercer implements DirectCoercer<CorpGroupUrn> {

    private static final boolean REGISTER_COERCER = Custom.registerCoercer(new CorpGroupUrnCoercer(), CorpGroupUrn.class);

    public CorpGroupUrnCoercer() {
    }

    public Object coerceInput(CorpGroupUrn object) throws ClassCastException {
        return object.toString();
    }

    public CorpGroupUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
            return CorpGroupUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
            throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
    }
}
