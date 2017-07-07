package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by rahulv on 6/21/17.
 */
public class PostgresTypes {
    public static final String TEXT_ARRAY_LOGICAL_NAME = "TEXTARRAY";
    //public static final String SCALE_FIELD = "scale";

    public static final String INTEGER_ARRAY_LOGICAL_NAME = "INTARRAY";

    public static final String JSONB_LOGICAL_NAME = "JSONB";

    public static final String POINT_LOGICAL_NAME = "POINT";

    public static final String CIDR_LOGICAL_NAME = "CIDR";

    public static final String INET_LOGICAL_NAME = "INET";
    /**
     * Returns a SchemaBuilder for a Decimal with the given scale factor. By returning a SchemaBuilder you can override
     * additional schema settings such as required/optional, default value, and documentation.
     *
     * @return a SchemaBuilder
     */

    public static SchemaBuilder TextArrayBuilder() {
        return SchemaBuilder.string().name(TEXT_ARRAY_LOGICAL_NAME).version(Integer.valueOf(1));
    }

    public static SchemaBuilder JsonbBuilder(){
        return SchemaBuilder.string().name(JSONB_LOGICAL_NAME).version(Integer.valueOf(1));
    }

    public static SchemaBuilder IntArrayBuilder(){
        return SchemaBuilder.string().name(INTEGER_ARRAY_LOGICAL_NAME).version(Integer.valueOf(1));
    }

    public static SchemaBuilder CidrBuilder(){
        return SchemaBuilder.string().name(CIDR_LOGICAL_NAME).version(Integer.valueOf(1));
    }

    public static SchemaBuilder PointBuilder(){
        return SchemaBuilder.string().name(POINT_LOGICAL_NAME).version(Integer.valueOf(1));
    }

    public static SchemaBuilder InetBuilder(){
        return SchemaBuilder.string().name(INET_LOGICAL_NAME).version(Integer.valueOf(1));
    }

}
