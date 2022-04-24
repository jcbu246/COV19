package cs505finaltemplate.Topics;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.graphDB.GraphDBEngine;

import java.awt.*;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopicConnector {

    private Gson gson;

    final Type typeOfPatientData = new TypeToken<List<PatientData>>(){}.getType();
    final Type typeOfHospitalData = new TypeToken<List<HospitalData>>(){}.getType();
    final Type typeOfVaccinationData = new TypeToken<List<VaccinationData>>(){}.getType();

    Map<String,String> config;

    public TopicConnector(Map<String,String> config) {
        gson = new Gson();
        this.config = config;
    }

    public void connect() {

        try {

            //create connection factory, this can be used to create many connections
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.get("hostname"));
            factory.setUsername(config.get("username"));
            factory.setPassword(config.get("password"));
            factory.setVirtualHost(config.get("virtualhost"));

            //create a connection, many channels can be created from a single connection
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            patientListChannel(channel);
            hospitalListChannel(channel);
            vaxListChannel(channel);

        } catch (Exception ex) {
            System.out.println("connect Error: " + ex.getMessage());
            ex.printStackTrace();
        }
}

    private void patientListChannel(Channel channel) {
        try {

            System.out.println("Creating patient_list channel");

            String topicName = "patient_list";

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Patient List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");

                List<PatientData> incomingList = gson.fromJson(message, typeOfPatientData);
                GraphDBEngine.openConnection();
                for (PatientData patientData : incomingList) {

                    //Only send positive cases into CEP
                    if (patientData.patient_status == 1){
                        //Data to send to CEP
                        Map<String,String> zip_entry = new HashMap<>();
                        zip_entry.put("zip_code",String.valueOf(patientData.patient_zipcode));
                        String testInput = gson.toJson(zip_entry);

                        //insert into CEP
                        Launcher.cepEngine.input("testInStream",testInput);
                    }
                    //Send to DB engine for processing
                    GraphDBEngine.handlePatientData(patientData);

//                    System.out.println(patientData.patient_mrn);
//                    testing_id = 10
//                    patient_name = Kimberly Althouse
//                    patient_mrn = 45fe41fb-c352-11ec-90ac-0da233908077
//                    patient_zipcode = 42071
//                    patient_status = 0
//                    contact_list = [45fe41fb-c352-11ec-90ac-0da233908077, 45fe41fb-c352-11ec-90ac-0da233908077,
//                              45fe41fb-c352-11ec-90ac-0da233908077, 45fe41fb-c352-11ec-90ac-0da233908077,
//                              45fe41fb-c352-11ec-90ac-0da233908077]
//                    event_list = [45fe41fa-c352-11ec-90ac-0da233908077]
                }
                GraphDBEngine.closeConnection();
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception ex) {
            System.out.println("patientListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void hospitalListChannel(Channel channel) {
        try {

            String topicName = "hospital_list";

            System.out.println("Creating hospital_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Hospital List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                //new message
                String message = new String(delivery.getBody(), "UTF-8");
                //System.out.println(message);

                //convert string to class
                List<HospitalData> incomingList = gson.fromJson(message, typeOfHospitalData);
                GraphDBEngine.openConnection();
                for (HospitalData hospitalData : incomingList) {
                    //Send to DB engine to handle data
                    GraphDBEngine.handleHospitalData(hospitalData);
                }
                GraphDBEngine.closeConnection();
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("hospitalListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void vaxListChannel(Channel channel) {
        try {

            String topicName = "vax_list";

            System.out.println("Creating vax_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Vax List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                //System.out.println(message);

                //convert string to class
                List<VaccinationData> incomingList = gson.fromJson(message, typeOfVaccinationData);
                GraphDBEngine.openConnection();
                for (VaccinationData vaxData : incomingList) {
                    //Send to DB engine to handle vax data
                    GraphDBEngine.handleVaccinationData(vaxData);
                }
                GraphDBEngine.closeConnection();
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("vaxListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

}
