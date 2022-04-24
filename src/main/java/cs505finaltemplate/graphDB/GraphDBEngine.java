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

public class GraphDBEngine {

    private static OrientDB client = null;
    private static ODatabaseSession db = null;

    //region Constructor
    public GraphDBEngine() {
        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //Open connection to database client
        openClient();

        //Define database credentials
        String database = "finalproject";
        String username = "root";
        String password = "rootpwd";

        // Reset DB and create new one
        resetDB(database, username, password);

        // Open database session
        openDB(database, username, password);

        // Setup database
        setupDB();

        //Close database connections
        closeDB();
        closeClient();
    }
    //endregion

    //region Public Methods

    /**
     * Opens the connection to the default database.
     */
    public static void openConnection(){
        openClient();
        openDB("finalproject", "root" , "rootpwd");
    }

    /**
     * Closes the connection to the default database.
     */
    public static void closeConnection() {
        closeDB();
        closeClient();
    }

    /**
     * Handles all the patient data for the database.
     * @param patient Patient Data object.
     * @return true on success, false on failure.
     */
    public static boolean handlePatientData(PatientData patient) {
        try {
            //Add patient to database
            OVertex vPatient = createPatient(patient);

            //Update contacts
            updateContacts(patient, vPatient);

            //Update event contacts
            updateEvents(patient, vPatient);
        } catch (Exception ex) {
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
            //Check if hospital is new

            //if new, add to database, add patient hospital status to patient

            //if existing, add patient hospital status to patient
        } catch (Exception ex) {
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

        } catch (Exception ex) {
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
        openClient();
        int result = resetDB("finalproject", "root", "rootpwd");
        closeClient();
        return result;
    }
    //endregion

    //region Private Methods
    /**
     * Open the connection to OrientDB client
     */
    private static void openClient() {
        if (client == null) {
            client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
        }
    }

    /**
     * Close the connection to OrientDB client
     */
    private static void closeClient() {
        client.close();
        client = null;
    }

    private static void openDB(String database, String username, String password) {
        if (client != null && db == null) {
            db = client.open(database, username, password);
        }
    }

    private static void closeDB() {
        db.close();
        db = null;
    }

    /**
     * Resets the database by dropping the database and creating a new one.
     * @param database name of database to reset
     */
    private static int resetDB(String database, String username, String password) {
        try {
            if (client.exists(database)) {
                client.drop(database);
            }
            //Create the database
            client.create(database, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());

            // Open database session
            openDB(database, username, password);

            //Setup database
            if (setupDB() != 1) {return 0;}

            //Close connection
            closeDB();

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
    private static int setupDB() {
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
                event.createProperty("event_id", OType.INTEGER);
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

            //create patient_to_testing connection class
            if (db.getClass("patient_to_testing") == null) {
                db.createEdgeClass("patient_to_testing");
            }

            //create patient_to_hospital connection class
            if (db.getClass("patient_to_hospital") == null) {
                db.createEdgeClass("patient_to_hospital");
            }

            //create patient_to_vaccination connection class
            if (db.getClass("patient_to_vaccination") == null) {
                db.createEdgeClass("patient_to_vaccination");
            }

            return 1;
        } catch (Exception ex) {
            System.out.println(ex);
            return 0;
        }
    }

    private static void testData() {
        OVertex patient_0 = createPatient("mrn_0");
        OVertex patient_1 = createPatient("mrn_1");
        OVertex patient_2 = createPatient("mrn_2");
        OVertex patient_3 = createPatient("mrn_3");

        //patient 0 in contact with patient 1
        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
        edge1.save();
        //patient 2 in contact with patient 0
        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
        edge3.save();

        getContacts("mrn_0");
    }

    private static OVertex createPatient(PatientData patient) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient.patient_mrn);
        result.setProperty("patient_name", patient.patient_name);
        result.setProperty("patient_zipcode", patient.patient_zipcode);
        result.setProperty("patient_covid_status", patient.patient_status);
        result.setProperty("testing_id", patient.testing_id);
        result.save();
        return result;
    }

    private static OVertex createPatient(String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
    }

    private static OVertex createEvent(String event_id) {
        OVertex result = db.newVertex("event");
        result.setProperty("event_id", event_id);
        result.save();
        return result;
    }

    private static void updateContacts(PatientData patientData, OVertex vPatient) {
        for (String contact : patientData.contact_list) {
            //Add the patient to the database
            OVertex vContact = createPatient(contact);

            //Add connection between patients
            OEdge edge = vPatient.addEdge(vContact, "patient_to_patient");
            edge.save();
        }
    }

    private static void updateEvents(PatientData patientData, OVertex vPatient) {
        for (String event : patientData.event_list) {
            //Add the event to the database
            OVertex vEvent = createEvent(event);

            //Add connection between patient and event
            OEdge edge = vPatient.addEdge(vEvent, "patient_to_event");
            edge.save();
        }
    }

    private static void getContacts(String patient_mrn) {

        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }
    //endregion
}
