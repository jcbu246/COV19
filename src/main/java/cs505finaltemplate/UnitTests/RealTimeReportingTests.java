package cs505finaltemplate.UnitTests;
import cs505finaltemplate.CEP.OutputSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RealTimeReportingTests {
    public static int[] alertZipcodeList = null;
    public static int numAlertedZipcodes = 0;

    @Test
    public void testRTR1_NoData() {
        String testMessage = "{}";

        OutputSubscriber messageReceiver = new OutputSubscriber("MF1", "Test");
        messageReceiver.onMessage(testMessage);

        Assertions.assertArrayEquals(alertZipcodeList, new int[0]);
        Assertions.assertEquals(numAlertedZipcodes, 0);
    }

    @Test
    public void testRTR1_DataNoAlert() {
        String testMessage = "[{\"event\":{\"zip_code\":\"41056\",\"count\":1}},{\"event\":{\"zip_code\":\"40504\",\"count\":1}},{\"event\":{\"zip_code\":\"41240\",\"count\":1}}]";

        OutputSubscriber messageReceiver = new OutputSubscriber("MF1", "Test");
        messageReceiver.onMessage(testMessage);

        Assertions.assertArrayEquals(alertZipcodeList, new int[0]);
        Assertions.assertEquals(numAlertedZipcodes, 0);
    }

    @Test
    public void testRTR1_DataAlert() {
        String testMessage1 = "[{\"event\":{\"zip_code\":\"41056\",\"count\":1}},{\"event\":{\"zip_code\"" +
                ":\"40504\",\"count\":1}},{\"event\":{\"zip_code\":\"41240\",\"count\":1}}]";
        String testMessage2 = "[{\"event\":{\"zip_code\":\"41056\",\"count\":3}},{\"event\":{\"zip_code\"" +
                ":\"40504\",\"count\":1}},{\"event\":{\"zip_code\":\"41240\",\"count\":2}}]";
        OutputSubscriber messageReceiver = new OutputSubscriber("MF1", "Test");
        messageReceiver.onMessage(testMessage1);
//        System.out.println(alertZipcodeList);
//        System.out.println(numAlertedZipcodes);
        Assertions.assertArrayEquals(alertZipcodeList, new int[0]);
        Assertions.assertEquals(numAlertedZipcodes, 0);

        messageReceiver.onMessage(testMessage2);
//        System.out.println(alertZipcodeList);
//        System.out.println(numAlertedZipcodes);
        Assertions.assertArrayEquals(alertZipcodeList, new int[]{41056, 41240});
        Assertions.assertEquals(numAlertedZipcodes, 2);
    }

    @Test
    public void testRTR2_DataStateAlert() {
        String testMessage1 = "[{\"event\":{\"zip_code\":\"41056\",\"count\":1}},{\"event\":{\"zip_code\"" +
                ":\"40504\",\"count\":1}},{\"event\":{\"zip_code\":\"41240\",\"count\":1}}," +
                "{\"event\":{\"zip_code\":\"40001\",\"count\":1}}," +
                "{\"event\":{\"zip_code\":\"40002\",\"count\":1}}]";
        String testMessage2 = "[{\"event\":{\"zip_code\":\"41056\",\"count\":3}},{\"event\":{\"zip_code\"" +
                ":\"40504\",\"count\":3}},{\"event\":{\"zip_code\":\"41240\",\"count\":3}}," +
                "{\"event\":{\"zip_code\":\"40001\",\"count\":3}}," +
                "{\"event\":{\"zip_code\":\"40002\",\"count\":3}}]";
        OutputSubscriber messageReceiver = new OutputSubscriber("MF1", "Test");
        messageReceiver.onMessage(testMessage1);
        messageReceiver.onMessage(testMessage2);
        System.out.println(numAlertedZipcodes);
        Assertions.assertTrue(numAlertedZipcodes >= 5);
    }
}
