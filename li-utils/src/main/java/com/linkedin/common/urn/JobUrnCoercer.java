package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;

public class JobUrnCoercer implements DirectCoercer<JobUrn> {
    private static final boolean REGISTER_COERCER = Custom.registerCoercer(new JobUrnCoercer(), JobUrn.class);

    public JobUrnCoercer() {
    }

    @Override
    public Object coerceInput(JobUrn object) throws ClassCastException {
        return object.toString();
    }

    public JobUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
            return JobUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
            throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
    }
}
