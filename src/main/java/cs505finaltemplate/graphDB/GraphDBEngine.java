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

public class GraphDBEngine {

    private static OrientDB client = null;


    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine() {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //Open connection to database client
        //client = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        client = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());

        // Reset DB and create new one
        int result = resetDB("finalproject");
        System.out.println(result);

        // Get the database session
        ODatabaseSession db = client.open("finalproject", "root", "rootpwd");

        //clearDB(db);

        //create classes
        OClass patient = db.getClass("patient");

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
        }


        OVertex patient_0 = createPatient(db, "mrn_0");
        OVertex patient_1 = createPatient(db, "mrn_1");
        OVertex patient_2 = createPatient(db, "mrn_2");
        OVertex patient_3 = createPatient(db, "mrn_3");

        //patient 0 in contact with patient 1
        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
        edge1.save();
        //patient 2 in contact with patient 0
        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
        edge3.save();

        getContacts(db, "mrn_0");

        db.close();
        client.close();

    }

    private static OVertex createPatient(ODatabaseSession db, String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
    }

    private static void getContacts(ODatabaseSession db, String patient_mrn) {

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

    /**
     * Clears all data from the graph database and makes a clean slate.
     * @param db the current database object to clear
     */
    private static void clearDB(ODatabaseSession db) {
        String query = "DELETE VERTEX FROM patient";
        db.command(query);
    }

    /**
     * Resets the database by dropping the database and creating a new one.
     * @param name
     */
    public static int resetDB(String name) {
        try {
            if (client.exists(name)) {
                client.drop(name);
            }
            client.create(name, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
            return 1;
        } catch (Exception ex) {
            System.out.println(ex);
            return 0;
        }
    }
}
