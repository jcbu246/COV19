package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import cs505finaltemplate.httpcontrollers.API;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;
    private Map<Integer, Integer> prevZipAlertEvent;
    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
        this.prevZipAlertEvent = new HashMap<>();
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
            String parsedMessage = String.valueOf(msg).replaceAll("\\[|\\{|}|]|\"event\":", "");
            parsedMessage = parsedMessage.replaceAll("\"zip_code\":|\"count\":|\"", "");
            String[] splitMessage = parsedMessage.split(",");
            //System.out.println(parsedMessage);

            //Build Alert String for each zipcode
            for (int i=0; i < splitMessage.length-1; i+=2){
                Integer zipcode = Integer.parseInt(splitMessage[i]);
                Integer newCount = Integer.parseInt(splitMessage[i+1]);

                //Check if zip was in previous event
                if (prevZipAlertEvent.containsKey(zipcode)) {
                    //Check if zip has doubled in size
                    if (prevZipAlertEvent.get(zipcode) * 2 <= newCount) {
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
            //System.out.println(alertZipcodeList.toString());

            //Send alert list to API
            API.numAlertedZipcodes = alertZipcodeList.size();
            API.alertZipcodeList = alertZipcodeList.stream().mapToInt(Integer::intValue).toArray();

           
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
