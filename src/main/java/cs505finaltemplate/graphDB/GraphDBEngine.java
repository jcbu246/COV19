package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.PatientData;
import cs505finaltemplate.Topics.VaccinationData;

import java.util.*;

public class GraphDBEngine {
    private static String database_name = "finalproject";
    private static String username = "root";
    private static String password = "rootpwd";

    //region Constructor
    public GraphDBEngine() {
        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //Open connection to database client
        OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());

        // Reset DB and create new one
        resetDB(client);

        client.close();
    }
    //endregion

    //region Public Methods
    /**
     * Handles all the patient data for the database.
     * @param patient Patient Data object.
     * @return true on success, false on failure.
     */
    public static boolean handlePatientData(PatientData patient) {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());
            //Add the patient to the database
            Optional<OVertex> temp = getPatient(db, patient.patient_mrn);
            OVertex vPatient = null;
            if (temp.isPresent())
                vPatient = updatePatient(patient, temp.get());
            else
                vPatient = createPatient(db, patient);

            //Update testing facility
            updateTestingFacility(db, patient, vPatient);

            //Update contacts
            updateContacts(db, patient, vPatient);

            //Update event contacts
            updateEvents(db, patient, vPatient);

            //close database connection
            db.close();
            client.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return false;
        }
        return true;
    }

    /**
     * Handles all the hospital data for the database.
     * @param hospital Hospital Data object.
     * @return true on success, false on failure.
     */
    public static boolean handleHospitalData(HospitalData hospital) {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());
            //get patient record or add to database
            Optional<OVertex> temp = getPatient(db, hospital.patient_mrn);
            OVertex vPatient = null;
            if (temp.isPresent())
                vPatient = updatePatient(hospital, temp.get());
            else
                vPatient = createPatient(db, hospital);

            //update hospital connection
            updateHospitalFacility(db, hospital, vPatient);

            //Close database connection
            db.close();
            client.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return false;
        }
        return true;
    }

    /**
     * Handles all the vaccination data for the database.
     * @param vaccination Vaccination Data object.
     * @return true on success, false on failure.
     */
    public static boolean handleVaccinationData(VaccinationData vaccination) {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());
            //get patient record or add to database
            Optional<OVertex> temp = getPatient(db, vaccination.patient_mrn);
            OVertex vPatient = null;
            if (temp.isPresent())
                vPatient = updatePatient(vaccination, temp.get());
            else
                vPatient = createPatient(db, vaccination);

            //update hospital connection
            updateVaccinationFacility(db, vaccination, vPatient);

            //Close Connection to Database
            db.close();
            client.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return false;
        }
        return true;
    }

    /**
     * Reset default database connection
     * @return 1 on success, 0 on failure.
     */
    public static int reset() {
        int result = -1;
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            result = resetDB(client);
            client.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public static Map<String, List<String>> getPotentialContacts(String patient_mrn) {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());

            String query1 = "Traverse inE(), outE(), inV(), outV() FROM " +
                    "(select * from patient where patient_mrn = ?) " +
                    "while $depth <= 2";
            OResultSet rs1 = db.query(query1, patient_mrn);

            Map<String, List<String>> list = new HashMap<>();
            String event_id = "";
            while (rs1.hasNext()) {
                OResult item1 = rs1.next();
                if (item1.hasProperty("event_id"))
                    event_id = item1.getProperty("event_id");
                if (event_id != "") {
                    String query = "TRAVERSE inE(), outE(), inV(), outV() FROM " +
                            "(select * from event where event_id = ?) " +
                            "WHILE $depth <= 2";
                    OResultSet rs = db.query(query, event_id);

                    String currentEvent = null;
                    List<String> currentMRNs = new ArrayList<>();
                    while (rs.hasNext()) {
                        OResult item = rs.next();
                        if (item.hasProperty("event_id")) {
                            //Check if first event in the result set
                            String temp = item.getProperty("event_id");
                            if (currentEvent == null)
                                currentEvent = temp;
                            else if (temp.equals(currentEvent))
                                list.put(currentEvent, currentMRNs);
                        }
                        if (item.hasProperty("patient_mrn")) {
                            String temp = item.getProperty("patient_mrn");
                            if (!temp.equals(patient_mrn))
                                currentMRNs.add(item.getProperty("patient_mrn"));
                        }
                    }
                    if (currentEvent != null && currentMRNs.size() > 0)
                        list.put(currentEvent, currentMRNs);
                    rs.close();
                }
            }
            rs1.close();

            //Close databse connection
            db.close();
            client.close();
            return list;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return new HashMap<>();
        }
    }

    public static List<String> getConfirmedContacts(String patient_mrn) {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());

            String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                    "FROM (select from patient where patient_mrn = ?) " +
                    "WHILE $depth <= 2";
            OResultSet rs = db.query(query, patient_mrn);

            List<String> resultList = new ArrayList<>();
            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.hasProperty("patient_mrn")) {
                    String temp = item.getProperty("patient_mrn");
                    if (!patient_mrn.equals(temp)) {
                        resultList.add(temp);
                    }
                }
            }
            rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!

            //Close connection to database
            db.close();
            client.close();
            return resultList;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return new ArrayList<>();
        }
    }

    public static Map<String, Float> getPatientStatus(int hospital_id) {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());

            // Get patients that attend hospital
            String query = "select patient_vaccination_status, patient_hospital_status from patient where hospital_id = ?";
            OResultSet rs = db.query(query, hospital_id);

            List<Integer> vaxStatusList = new ArrayList<>();
            List<Integer> patientStatusList = new ArrayList<>();
            while (rs.hasNext()) {
                OResult item = rs.next();
                if (item.hasProperty("patient_vaccination_status"))
                    vaxStatusList.add(item.getProperty("patient_vaccination_status"));
                if (item.hasProperty("patient_hospital_status"))
                    patientStatusList.add(item.getProperty("patient_hospital_status"));
            }
            rs.close();

            //Close database connection
            db.close();
            client.close();

            return calculateStats(vaxStatusList, patientStatusList);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return new HashMap<>();
        }
    }

    public static Map<Integer, Map<String,Float>> getAllPatientStatus() {
        try {
            OrientDB client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());

            // Get patients that attend hospital
            String query = "select hospital_id, patient_hospital_status, patient_vaccination_status from patient where hospital_id is not null order by hospital_id";
            OResultSet rs = db.query(query);

            Map<Integer, Map<String, Float>> result = new HashMap<>();
            List<Integer> vaxStatusList = new ArrayList<>();
            List<Integer> patientStatusList = new ArrayList<>();
            Integer hospital_id = -1;
            while (rs.hasNext()) {
                OResult item = rs.next();
                if (hospital_id == -1 && item.hasProperty("hospital_id")) {
                    hospital_id = item.getProperty("hospital_id");
                }
                else if (hospital_id != item.getProperty("hospital_id")) {
                    //Build list and add it to map
                    result.put(hospital_id, calculateStats(vaxStatusList, patientStatusList));
                    vaxStatusList.clear();
                    patientStatusList.clear();
                    hospital_id = item.getProperty("hospital_id");
                }
                if (item.hasProperty("patient_vaccination_status"))
                    vaxStatusList.add(item.getProperty("patient_vaccination_status"));
                if (item.hasProperty("patient_hospital_status"))
                    patientStatusList.add(item.getProperty("patient_hospital_status"));
            }
            rs.close();
            result.put(hospital_id, calculateStats(vaxStatusList, patientStatusList));

            db.close();
            client.close();

            if (hospital_id == -1) {
                return new HashMap<>();
            }
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex);
            return new HashMap<>();
        }
    }
    //endregion

    //region Private Methods
    //region Helper Methods
    private static Map<String, Float> calculateStats(List<Integer> vaxStatusList, List<Integer> patientStatusList) {
        Map<String, Float> results = new HashMap<>();
        //Calculate output values
        Integer inPatientCount = 0;
        Integer inPatientVaxCount = 0;
        Integer icuCount = 0;
        Integer icuVaxCount = 0;
        Integer ventCount = 0;
        Integer ventVaxCount = 0;
        for (int i=0; i<patientStatusList.size(); i++) {
            //In-Patient Stats
            if (patientStatusList.get(i) == 1) {
                inPatientCount++;
                if (vaxStatusList.get(i) == 1) {
                    inPatientVaxCount++;
                }
            }
            //ICU Stats
            else if (patientStatusList.get(i) == 2) {
                icuCount++;
                if (vaxStatusList.get(i) == 1) {
                    icuVaxCount++;
                }
            }
            //Vent Stats
            else if (patientStatusList.get(i) == 3) {
                ventCount++;
                if (vaxStatusList.get(i) == 1) {
                    ventVaxCount++;
                }
            }
        }
        Float inPatientVaxPercentage = 0f;
        if (inPatientCount != 0)
            inPatientVaxPercentage = inPatientVaxCount.floatValue() / inPatientCount.floatValue();
        Float icuVaxPercentage = 0f;
        if (icuCount != 0)
            icuVaxPercentage = icuVaxCount.floatValue() / icuCount.floatValue();
        Float ventVacPercentage = 0f;
        if (ventCount != 0)
            ventVacPercentage = ventVaxCount.floatValue() / ventCount.floatValue();

        //Add results to map
        results.put("in-patient_count", inPatientCount.floatValue());
        results.put("in-patient_vax", inPatientVaxPercentage);
        results.put("icu-patient_count", icuCount.floatValue());
        results.put("icu-patient_vax", icuVaxPercentage);
        results.put("patient_vent_count", ventCount.floatValue());
        results.put("patient_vent_vax", ventVacPercentage);
        return results;
    }
    //endregion
    //region Database Management Methods
    /**
     * Resets the database by dropping the database and creating a new one.
     */
    private static int resetDB(OrientDB client) {
        try {
            if (client.exists(database_name)) {
                client.drop(database_name);
                System.out.println("Database dropped and reset.");
            }
            //Create the database
            client.create(database_name, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());

            // Open database session
            ODatabaseSession db = client.open(database_name, username, password, OrientDBConfig.defaultConfig());

            //Setup database
            setupDB(db);

            //Close connection
            db.close();

            return 1;
        } catch (Exception ex) {
            System.out.println(ex);
            return 0;
        }
    }

    /**
     * Setup the database for the data we need to capture
     * @return 1 on success; 0 on fail
     */
    private static void setupDB(ODatabaseSession db) {
        try {
            //create patient class
            OClass patient = db.getClass("patient");
            if (patient == null) {
                patient = db.createVertexClass("patient");
            }
            if (patient.getProperty("patient_mrn") == null) {
                patient.createProperty("testing_id", OType.INTEGER);
                patient.createProperty("hospital_id", OType.INTEGER);
                patient.createProperty("vaccination_id", OType.INTEGER);
                patient.createProperty("patient_mrn", OType.STRING);
                patient.createIndex("patient_mrn_index", OClass.INDEX_TYPE.UNIQUE, "patient_mrn");
                patient.createProperty("patient_name", OType.STRING);
                patient.createProperty("patient_zipcode", OType.INTEGER);
                patient.createProperty("patient_covid_status", OType.INTEGER);
                patient.createProperty("patient_hospital_status", OType.INTEGER);
                patient.createProperty("patient_vaccination_status", OType.INTEGER);
            }

            //create hospital class
            OClass hospital = db.getClass("hospital");
            if (hospital == null){
                hospital = db.createVertexClass("hospital");
            }
            if (hospital.getProperty("hospital_id") == null) {
                hospital.createProperty("hospital_id", OType.INTEGER);
                hospital.createIndex("hospital_id_index", OClass.INDEX_TYPE.UNIQUE, "hospital_id");
            }

            //create event class
            OClass event = db.getClass("event");
            if (event == null) {
                event = db.createVertexClass("event");
            }
            if (event.getProperty("event_id") == null) {
                event.createProperty("event_id", OType.STRING);
                event.createIndex("event_id_index", OClass.INDEX_TYPE.UNIQUE, "event_id");
            }

            //create patient-to-patient contact class
            if (db.getClass("patient_to_patient") == null) {
                db.createEdgeClass("patient_to_patient");
            }

            //create patient-to-event contact class
            if (db.getClass("patient_to_event") == null) {
                db.createEdgeClass(("patient_to_event"));
            }

            //create patient_to_hospital connection class
            if (db.getClass("patient_to_hospital") == null) {
                db.createEdgeClass("patient_to_hospital");
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }
    //endregion
    //region Database CRUD Methods
    private static OVertex createPatient(ODatabaseSession db, PatientData patient) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient.patient_mrn);
        result.setProperty("patient_name", patient.patient_name);
        result.setProperty("patient_zipcode", patient.patient_zipcode);
        result.setProperty("patient_covid_status", patient.patient_status);
        result.setProperty("testing_id", patient.testing_id);
        result.setProperty("patient_vaccination_status", 0);
        result.setProperty("patient_hospital_status", 0);
        result.save();
        return result;
    }

    private static OVertex createPatient(ODatabaseSession db, HospitalData hospitalData) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", hospitalData.patient_mrn);
        result.setProperty("patient_name", hospitalData.patient_name);
        result.setProperty("patient_hospital_status", hospitalData.patient_status);
        result.setProperty("hospital_id", hospitalData.hospital_id);
        result.setProperty("patient_vaccination_status", 0);
        result.save();
        return result;
    }

    private static OVertex createPatient(ODatabaseSession db, VaccinationData vaccinationData) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", vaccinationData.patient_mrn);
        result.setProperty("patient_name", vaccinationData.patient_name);
        result.setProperty("hospital_id", vaccinationData.vaccination_id);
        result.setProperty("patient_vaccination_status", 1);
        result.setProperty("patient_hospital_status", 0);
        result.save();
        return result;
    }

    private static OVertex createPatient(ODatabaseSession db, String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.setProperty("patient_vaccination_status", 0);
        result.setProperty("patient_hospital_status", 0);
        result.save();
        return result;
    }

    private static OVertex createEvent(ODatabaseSession db, String event_id) {
        OVertex result = db.newVertex("event");
        result.setProperty("event_id", event_id);
        result.save();
        return result;
    }

    private static OVertex createHospital(ODatabaseSession db, int hospital_id) {
        OVertex result = db.newVertex("hospital");
        result.setProperty("hospital_id", hospital_id);
        result.save();
        return result;
    }

    private static void updateTestingFacility(ODatabaseSession db, PatientData patientData, OVertex patient) {
        //Add hospital to database
        Optional<OVertex> temp = getHospital(db, patientData.testing_id);
        OVertex vHospital = null;
        if (temp.isPresent())
            vHospital = temp.get();
        else
            vHospital = createHospital(db, patientData.testing_id);
    }

    private static void updateHospitalFacility(ODatabaseSession db, HospitalData hospitalData, OVertex patient) {
        //Add hospital to database
        Optional<OVertex> temp = getHospital(db, hospitalData.hospital_id);
        OVertex vHospital = null;
        if (temp.isPresent())
            vHospital = temp.get();
        else
            vHospital = createHospital(db, hospitalData.hospital_id);

        // connect Patient to testing facility
        OEdge edge = patient.addEdge(vHospital, "patient_to_hospital");
        edge.save();
    }

    private static void updateVaccinationFacility(ODatabaseSession db, VaccinationData vaccinationData, OVertex patient) {
        //Add hospital to database
        Optional<OVertex> temp = getHospital(db, vaccinationData.vaccination_id);
        OVertex vHospital = null;
        if (temp.isPresent())
            vHospital = temp.get();
        else
            vHospital = createHospital(db, vaccinationData.vaccination_id);
    }

    private static OVertex updatePatient(PatientData patientData, OVertex patient) {
        patient.setProperty("patient_name", patientData.patient_name);
        patient.setProperty("patient_zipcode", patientData.patient_zipcode);
        patient.setProperty("patient_covid_status", patientData.patient_status);
        patient.setProperty("testing_id", patientData.testing_id);
        patient.save();
        return patient;
    }

    private static OVertex updatePatient(HospitalData hospitalData, OVertex patient) {
        patient.setProperty("patient_name", hospitalData.patient_name);
        patient.setProperty("hospital_id", hospitalData.hospital_id);
        patient.setProperty("patient_hospital_status", hospitalData.patient_status);
        patient.save();
        return patient;
    }

    private static OVertex updatePatient(VaccinationData vaccinationData, OVertex patient) {
        patient.setProperty("patient_name", vaccinationData.patient_name);
        patient.setProperty("vaccination_id", vaccinationData.vaccination_id);
        patient.setProperty("patient_vaccination_status", 1);
        patient.save();
        return patient;
    }

    private static void updateContacts(ODatabaseSession db, PatientData patientData, OVertex vPatient) {
        Map<String, Integer> seen = new HashMap<>();
        for (String contact : patientData.contact_list) {
            if (!seen.containsKey(contact)) {
                seen.put(contact, 1);
                //Add the patient to the database
                Optional<OVertex> temp = getPatient(db, contact);
                OVertex vContact = null;
                if (temp.isPresent())
                    vContact = temp.get();
                else
                    vContact = createPatient(db, contact);

                OEdge edge = vPatient.addEdge(vContact, "patient_to_patient");
                edge.save();
            }

        }
    }

    private static void updateEvents(ODatabaseSession db, PatientData patientData, OVertex vPatient) {
        Map<String, Integer> seen = new HashMap<>();
        for (String event : patientData.event_list) {
            if (!seen.containsKey(event)) {
                seen.put(event, 1);
                //Add the event to the database
                Optional<OVertex> temp = getEvent(db, event);
                OVertex vEvent = null;
                if (temp.isPresent())
                    vEvent = temp.get();
                else
                    vEvent = createEvent(db, event);

                OEdge edge = vPatient.addEdge(vEvent, "patient_to_event");
                edge.save();
            }
        }
    }
    //endregion
    //region Private Database Query Methods
    private static Optional<OVertex> getPatient(ODatabaseSession db, String patient_mrn) {
        String query = "select * from patient where patient_mrn = ?";
        OResultSet rs = db.query(query, patient_mrn);

        Optional<OVertex> vPatient = Optional.empty();
        if (rs.hasNext()) {
            OResult item = rs.next();
            vPatient = item.getVertex();
        }
        rs.close();
        return vPatient;
    }

    private static Optional<OVertex> getEvent(ODatabaseSession db, String event_id) {
        String query = "select * from event where event_id = ?";
        OResultSet rs = db.query(query, event_id);

        Optional<OVertex> vEvent = Optional.empty();
        if (rs.hasNext()) {
            OResult item = rs.next();
            vEvent = item.getVertex();
        }
        rs.close();
        return vEvent;
    }

    private static Optional<OVertex> getHospital(ODatabaseSession db, int hospital_id) {
        String query = "select * from hospital where hospital_id = ?";
        OResultSet rs = db.query(query, hospital_id);

        Optional<OVertex> vHospital = Optional.empty();
        if (rs.hasNext()) {
            OResult item = rs.next();
            vHospital = item.getVertex();
        }
        rs.close();
        return vHospital;
    }
    //endregion
    //endregion
}
