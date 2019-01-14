package com.epam.udtf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A UDTF that takes in 1 argument (a user agent string) and converts it into (device, os, browser) columns.
 */
public class ParseUserAgentUDTF extends GenericUDTF {

    /**
     * Initializes the argument inspector. Which sets the type of new columns and their names.
     *
     * @param argOIs a collective inspector, from which you can derive the type of column, passed as the argument.
     * @return a new object inspector for the new fields.
     * @throws UDFArgumentException in case when the input is bad.
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        checkArguments(argOIs);
        return constructNewObjectInspector();
    }

    /**
     * This method checks the validity of input arguments to our UDTF.
     *
     * @param argOIs argument object inspectors.
     * @throws UDFArgumentException in case when the input is bad.
     */
    private void checkArguments(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();

        final int ONE_ELEMENT = 1;
        if (inputFields.size() != ONE_ELEMENT) {
            throw new UDFArgumentException("com.epam.udtf.ParseUserAgentUDTF() takes exactly one argument");
        }

        final int FIRST_INSPECTOR = 0;
        ObjectInspector udtfInputOI = inputFields.get(FIRST_INSPECTOR).getFieldObjectInspector();
        PrimitiveObjectInspector primitiveUDTFOI = (PrimitiveObjectInspector) udtfInputOI;

        if (udtfInputOI.getCategory() != ObjectInspector.Category.PRIMITIVE
                && primitiveUDTFOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("com.epam.udtf.ParseUserAgentUDTF() takes a string as a parameter");
        }
    }

    /**
     * This method constructs a new object inspector with names and types of our new columns.
     *
     * @return a newly created object inspector.
     */
    private StructObjectInspector constructNewObjectInspector() {
        List<String> fieldNames = Arrays.asList("device", "os", "browser");
        final int INITIAL_CAPACITY = 3;
        List<ObjectInspector> fieldOIs = new ArrayList<>(INITIAL_CAPACITY);
        for (int i = 0; i < INITIAL_CAPACITY; i++) {
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * This method takes in the objects, passed to the UDTF, transforms them into more columns and rows.
     *
     * @param objects the input objects.
     * @throws HiveException an exception encountered during any hive operations.
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        final int FIRST_OBJECT = 0;
        final String userAgentString = objects[FIRST_OBJECT].toString();
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
        Object[] result = new Object[]{
                userAgent.getOperatingSystem().getDeviceType().getName(),
                userAgent.getOperatingSystem().getName(),
                userAgent.getBrowser().getName()
        };
        forward(result);
    }

    /**
     * Called to notify the UDTF that there are no more rows to process.
     *
     * @throws HiveException an exception encountered during any hive operations.
     */
    @Override
    public void close() throws HiveException {
        // no implementation needed.
    }
}
