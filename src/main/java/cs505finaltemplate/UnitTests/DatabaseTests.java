package cs505finaltemplate.UnitTests;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.PatientData;
import cs505finaltemplate.Topics.VaccinationData;
import cs505finaltemplate.graphDB.GraphDBEngine;
import org.junit.jupiter.api.*;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class DatabaseTests {

    private static GraphDBEngine graphDBEngine;
    final Type typeOfPatientData = new TypeToken<List<PatientData>>(){}.getType();
    final Type typeOfHospitalData = new TypeToken<List<HospitalData>>(){}.getType();
    final Type typeOfVaccinationData = new TypeToken<List<VaccinationData>>(){}.getType();

    @Test
    public void testNewPatientData() {
        String data = "[{\"testing_id\" = 201," +
                "\"patient_name\" = \"Kimberly Althouse\"," +
                "\"patient_mrn\" = \"001\"," +
                "\"patient_zipcode\" = 40000," +
                "\"patient_status\" = 0," +
                "\"contact_list\" = [\"002\",\"003\", \"004\"]," +
                "\"event_list\" = [\"100\", \"300\"]}," +
                "{\"testing_id\" = 200," +
                "\"patient_name\" = \"John Doe\"," +
                "\"patient_mrn\" = \"002\"," +
                "\"patient_zipcode\" = 40001," +
                "\"patient_status\" = 0," +
                "\"contact_list\" = [\"001\",\"003\"]," +
                "\"event_list\" = [\"100\", \"200\", \"300\"]}," +
                "{\"testing_id\" = 200," +
                "\"patient_name\" = \"Mary Jane\"," +
                "\"patient_mrn\" = \"003\"," +
                "\"patient_zipcode\" = 40002," +
                "\"patient_status\" = 0," +
                "\"contact_list\" = [\"001\",\"002\", \"004\"]," +
                "\"event_list\" = [\"200\", \"300\"]}]";

        graphDBEngine = new GraphDBEngine();

        Gson gson = new Gson();
        List<PatientData> incomingList = gson.fromJson(data, typeOfPatientData);

        if (incomingList.size() > 0 ) {
            for (PatientData p : incomingList) {
                Assertions.assertTrue(GraphDBEngine.handlePatientData(p));
            }
        }
    }

    @Test
    public void testGetPotentialContacts() {
        Map<String, List<String>> result = GraphDBEngine.getPotentialContacts("001");

        Assertions.assertEquals("{100=[002], 300=[003, 002]}",result.toString());


    }

    @Test
    public void testGetConfirmedContacts() {
        List<String> result = GraphDBEngine.getConfirmedContacts("001");

        Assertions.assertEquals(result.toString(), "[004, 003, 002]");
    }

    @Test
    public void testNewHospitalData() {
        String data = "[{\"hospital_id\": \"201\",\"patient_name\": \"Kimberly Althouse\"," +
                "\"patient_mrn\": \"001\",\"patient_status\": 1}," +
                "{\"hospital_id\": \"200\",\"patient_name\": \"John Doe\"," +
                "\"patient_mrn\": \"002\",\"patient_status\": 3}," +
                "{\"hospital_id\": \"202\",\"patient_name\": \"Mary Jane\"," +
                "\"patient_mrn\": \"003\",\"patient_status\": 2}," +
                "{\"hospital_id\": \"203\",\"patient_name\": \"Bob Saget\"," +
                "\"patient_mrn\": \"004\",\"patient_status\": 1}]";

        Gson gson = new Gson();
        List<HospitalData> incomingList = gson.fromJson(data, typeOfHospitalData);

        if (incomingList.size() > 0 ) {
            for (HospitalData p : incomingList) {

                Assertions.assertTrue(GraphDBEngine.handleHospitalData(p));
            }
        }
    }

    @Test
    public void testNewVaccinationData() {
        String data = "[{\"vaccination_id\": 1,\"patient_name\": \"Kimberly Althouse\"," +
                "\"patient_mrn\": \"001\"}," +
                "{\"vaccination_id\": 1,\"patient_name\": \"John Doe\"," +
                "\"patient_mrn\": \"002\"}," +
                "{\"vaccination_id\": 3,\"patient_name\": \"Bob Saget\"," +
                "\"patient_mrn\": \"004\"}]";

        Gson gson = new Gson();
        List<VaccinationData> incomingList = gson.fromJson(data, typeOfVaccinationData);

        if (incomingList.size() > 0 ) {
            for (VaccinationData p : incomingList) {
                Assertions.assertTrue(GraphDBEngine.handleVaccinationData(p));
            }
        }
    }

    @Test
    public void testGetPatientStatusWithID() {
        Map<String, Float> results =  GraphDBEngine.getPatientStatus(200);

        Assertions.assertEquals("{patient_vent_count:=1.0, patient_vent_vax:=1.0, " +
                "in-patient_vax:=0.0, in-patient_count:=0.0, icu-patient_vax:=0.0, " +
                "icu-patient_count:=0.0}", results.toString());
    }

    @Test
    public void testGetPatientStatusAll() {
        Map<Integer, Map<String, Float>> results =  GraphDBEngine.getAllPatientStatus();

        Assertions.assertEquals("{200={patient_vent_count:=1.0, patient_vent_vax:=1.0, " +
                "in-patient_vax:=0.0, in-patient_count:=0.0, icu-patient_vax:=0.0, " +
                "icu-patient_count:=0.0}, 201={patient_vent_count:=0.0, patient_vent_vax:=0.0, " +
                "in-patient_vax:=1.0, in-patient_count:=1.0, icu-patient_vax:=0.0, icu-patient_count:=0.0}, " +
                "202={patient_vent_count:=0.0, patient_vent_vax:=0.0, in-patient_vax:=0.0, " +
                "in-patient_count:=0.0, icu-patient_vax:=0.0, icu-patient_count:=1.0}, " +
                "203={patient_vent_count:=0.0, patient_vent_vax:=0.0, in-patient_vax:=1.0, " +
                "in-patient_count:=1.0, icu-patient_vax:=0.0, icu-patient_count:=0.0}}", results.toString());
        //System.out.println(results.toString());
    }

}
