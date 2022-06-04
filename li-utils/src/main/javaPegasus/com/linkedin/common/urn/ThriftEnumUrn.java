package com.linkedin.common.urn;

import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;

public final class ThriftEnumUrn extends Urn {

    public static final String ENTITY_TYPE = "ThriftEnum";

    private final String _name;

    public ThriftEnumUrn(String name) {
        super(ENTITY_TYPE, TupleKey.create(name));
        this._name = name;
    }

    public String getNameEntity() {
        return _name;
    }

    public static ThriftEnumUrn createFromString(String rawUrn) throws URISyntaxException {
        return createFromUrn(Urn.createFromString(rawUrn));
    }

    public static ThriftEnumUrn createFromUrn(Urn urn) throws URISyntaxException {
        if (!"li".equals(urn.getNamespace())) {
            throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
        } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
            throw new URISyntaxException(urn.toString(), "Urn entity type should be 'ThriftEnum'.");
        } else {
            TupleKey key = urn.getEntityKey();
            if (key.size() != 1) {
                throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
            } else {
                try {
                    return new ThriftEnumUrn((String) key.getAs(0, String.class));
                } catch (Exception var3) {
                    throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
                }
            }
        }
    }

    public static ThriftEnumUrn deserialize(String rawUrn) throws URISyntaxException {
        return createFromString(rawUrn);
    }

    static {
        Custom.initializeCustomClass(ThriftEnumUrn.class);
        Custom.registerCoercer(new DirectCoercer<ThriftEnumUrn>() {
            public Object coerceInput(ThriftEnumUrn object) throws ClassCastException {
                return object.toString();
            }

            public ThriftEnumUrn coerceOutput(Object object) throws TemplateOutputCastException {
                try {
                    return ThriftEnumUrn.createFromString((String) object);
                } catch (URISyntaxException e) {
                    throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
                }
            }
        }, ThriftEnumUrn.class);
    }
}