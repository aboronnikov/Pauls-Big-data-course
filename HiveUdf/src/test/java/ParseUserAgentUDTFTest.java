import com.epam.udtf.ParseUserAgentUDTF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test for the com.epam.udtf.ParseUserAgentUDTF class.
 */
public class ParseUserAgentUDTFTest {

    /**
     * A collector that is necessary to make sure our process method correctly does it's job (makes new columns).
     * This is a mock class, that is used to extract the necessary objects.
     */
    private static class TestCollector implements Collector {
        /**
         * Device string cache.
         */
        private String device = null;

        /**
         * Os string cache.
         */
        private String os = null;

        /**
         * Browser string cache.
         */
        private String browser = null;

        /**
         * Collects the processed object into cache.
         *
         * @param collectee input object.
         * @throws HiveException an exception encountered during any hive operations.
         */
        @Override
        public void collect(Object collectee) throws HiveException {
            Object[] userInfo = (Object[]) collectee;
            final int DEVICE_ID = 0;
            final int OS_ID = 1;
            final int BROWSER_ID = 2;

            device = (String) userInfo[DEVICE_ID];
            os = (String) userInfo[OS_ID];
            browser = (String) userInfo[BROWSER_ID];
        }

        /**
         * Browser getter.
         *
         * @return browser string.
         */
        String getBrowser() {
            return browser;
        }

        /**
         * Device getter.
         *
         * @return device string.
         */
        String getDevice() {
            return device;
        }

        /**
         * Os getter.
         *
         * @return os string.
         */
        String getOs() {
            return os;
        }
    }

    /**
     * Object inspector, used by our tests.
     */
    private ArrayWritableObjectInspector objectInspector;

    /**
     * Object inspector, used by our tests.
     */
    private ArrayWritableObjectInspector badObjectInspector;

    /**
     * Test collector instance used by our tests.
     */
    private TestCollector testCollector;

    /**
     * com.epam.udtf.ParseUserAgentUDTF instance, used by our tests.
     */
    private ParseUserAgentUDTF parseUserAgentUDTF;

    /**
     * Sets up the object inspector.
     */
    @Before
    public void prepareObjectInspector() {
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        ArrayList<String> fieldNames = new ArrayList<>(
                Collections.singletonList("UserAgent")
        );
        structTypeInfo.setAllStructFieldNames(fieldNames);
        ArrayList<TypeInfo> typeInfos = new ArrayList<>(
                Collections.singletonList(TypeInfoFactory.stringTypeInfo)
        );
        structTypeInfo.setAllStructFieldTypeInfos(typeInfos);
        objectInspector = new ArrayWritableObjectInspector(structTypeInfo);
    }

    /**
     * Sets up the bad object inspector.
     */
    @Before
    public void prepareBadObjectInspector() {
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        ArrayList<String> fieldNames = new ArrayList<>();
        structTypeInfo.setAllStructFieldNames(fieldNames);
        ArrayList<TypeInfo> typeInfos = new ArrayList<>();
        structTypeInfo.setAllStructFieldTypeInfos(typeInfos);
        badObjectInspector = new ArrayWritableObjectInspector(structTypeInfo);
    }

    /**
     * Sets up the test collector.
     */
    @Before
    public void prepareTestCollector() {
        testCollector = new TestCollector();
    }

    /**
     * Sets up the UDTF.
     */
    @Before
    public void prepareUDTF() {
        parseUserAgentUDTF = new ParseUserAgentUDTF();
        parseUserAgentUDTF.setCollector(testCollector);
    }

    /**
     * This method tests the initialize method from com.epam.udtf.ParseUserAgentUDTF.
     *
     * @throws UDFArgumentException in case when the input is bad.
     */
    @Test
    public void initialize() throws UDFArgumentException {
        StructObjectInspector structObjectInspector = parseUserAgentUDTF.initialize(objectInspector);

        List<String> actualFieldNames = structObjectInspector.getAllStructFieldRefs()
                .stream()
                .map(StructField::getFieldName)
                .collect(Collectors.toList());

        List<ObjectInspector> actualFieldOIs = structObjectInspector.getAllStructFieldRefs()
                .stream()
                .map(StructField::getFieldObjectInspector)
                .collect(Collectors.toList());

        List<String> expectedFieldNames = Arrays.asList("device", "os", "browser");
        Assert.assertEquals(expectedFieldNames, actualFieldNames);

        actualFieldOIs.forEach(inspector -> {
            Assert.assertSame(PrimitiveObjectInspectorFactory.javaStringObjectInspector, inspector);
        });
    }

    /**
     * Tests initialize in the case of bad arguments.
     *
     * @throws UDFArgumentException in case when the input is bad.
     */
    @Test(expected = UDFArgumentException.class)
    public void badInitialize() throws UDFArgumentException {
        parseUserAgentUDTF.initialize(badObjectInspector);
    }

    /**
     * This method tests the process method from com.epam.udtf.ParseUserAgentUDTF.
     *
     * @throws HiveException an exception encountered during any hive operations.
     */
    @Test
    public void process() throws HiveException {
        Object[] objectsToProcess = {"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0),gzip(gfe),gzip(gfe) "};
        parseUserAgentUDTF.process(objectsToProcess);

        String expectedDevice = "Computer";
        String expectedOs = "Windows XP";
        String expectedBrowser = "Internet Explorer 8";

        Assert.assertEquals(expectedBrowser, testCollector.getBrowser());
        Assert.assertEquals(expectedDevice, testCollector.getDevice());
        Assert.assertEquals(expectedOs, testCollector.getOs());
    }
}