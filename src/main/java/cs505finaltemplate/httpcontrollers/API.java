package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.Topics.HospitalStatusData;
import cs505finaltemplate.graphDB.GraphDBEngine;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    public static int[] alertZipcodeList;
    public static int numAlertedZipcodes;

    //region MANAGEMENT FUNCTION ENDPOINTS
    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam() {
        String responseString = "{}";
        try {
            System.out.println("Team Member API Hit!");
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "Jake Overstreet");
            responseMap.put("Team_members_sids", "[12172151]");
            responseMap.put("app_status_code","1");

            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        try {
            int result = GraphDBEngine.reset();
            Map<String,Integer> responseMap = new HashMap<>();
            responseMap.put("reset_status_code", result);

            responseString = gson.toJson(responseMap);
        }
        catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
    //endregion

    //region REAL TIME REPORTING ENDPOINTS
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipAlertList() {
        String responseString = "{}";
        try {
            Map<String,int[]> responseMap = new HashMap<>();
            responseMap.put("ziplist", alertZipcodeList);

            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertList() {
        String responseString = "{}";
        try {
            Map<String, Integer> responseMap = new HashMap<>();
            //Check if state is on alert
            if (numAlertedZipcodes >= 5) {
                // Alert = 1, State is on alert
                responseMap.put("state_status", 1);
            }
            else {
                // Alert = 0, State is not on alert
                responseMap.put("state_status", 0);
            }

            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getconfirmedcontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfirmedContacts(@PathParam("mrn") String mrn) {
        String responseString = "{}";
        try {
            Map<String, List<String>> responseMap = new HashMap<>();
            List<String> result = GraphDBEngine.getConfirmedContacts(mrn);
            responseMap.put("contactlist", result);

            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpossiblecontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPossibleContacts(@PathParam("mrn") String mrn) {
        String responseString = "{}";
        try {
            Map<String, Map<String,List<String>>> responseMap = new HashMap<>();
            Map<String, List<String>> result = GraphDBEngine.getPotentialContacts(mrn);
            responseMap.put("contactlist", result);

            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
    //endregion

    //region OPERATIONAL REPORTING FUNCTIONS ENDPOINTS
    @GET
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus(@PathParam("hospital_id") int hospitalID) {
        String responseString = "{}";
        try {
            HospitalStatusData dataObj = GraphDBEngine.getPatientStatus(hospitalID);
            responseString = gson.toJson(dataObj);
            System.out.println(responseString);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus() {
        String responseString = "{}";
        try {
            HospitalStatusData dataObj = GraphDBEngine.getAllPatientStatus();
            responseString = gson.toJson(dataObj);
            System.out.println(responseString);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
}
