package cs505finaltemplate.CEP;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.httpcontrollers.API;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;
    private Map<Integer, Integer> prevZipAlertEvent;
    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            List<Integer> alertZipcodeList = new ArrayList<>();

            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            Launcher.lastCEPOutput = String.valueOf(msg);

            //Parse Event String
            //{"event":{"zip_code":"40143","count":2}}
            String[] event = String.valueOf(msg).split(":", 1);
            // "[{"event"", "{"zip_code":"40143","count":2}}""
            System.out.println(event);
            String[] values = event[1].split(",");

            //Build Alert String for each Zipcode
            for (String v : values) {
                String[] value = v.split(":");
                System.out.println(value);

                //Set variables for parsed zipcode and count
                Integer zipcode = Integer.getInteger(value[0]);
                Integer newCount = Integer.getInteger(value[1]);

                //Check if zip was in previous event
                if (prevZipAlertEvent.containsKey(zipcode)) {
                    //Check if zip has doubled in size
                    if (prevZipAlertEvent.get(zipcode) >= newCount * 2) {
                        //Add zip to alert list
                        alertZipcodeList.add(zipcode);
                    }

                    //Replace old count with new count in the list.
                    prevZipAlertEvent.replace(zipcode, newCount);
                }
                else {
                    //Wasn't in the last event. Add it to the list.
                    prevZipAlertEvent.put(zipcode, newCount);
                }
            }

            //Send alert list to API
            API.alertZipcodeList = alertZipcodeList.stream().mapToInt(Integer::intValue).toArray();


            //String[] sstr = String.valueOf(msg).split(":");
            //String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Long.parseLong(outval[0]);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
