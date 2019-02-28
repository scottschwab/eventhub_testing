/**
 * 
 */
package com.ctl.clc.analytics.eventsend;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author scott
 *
 */
public class Driver {

	public void connect(String namespace, String hubName, String sasKeyName, String saskey) 
               throws EventHubException, ExecutionException, InterruptedException, IOException {
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName(namespace) 
                .setEventHubName(hubName)
                .setSasKeyName(sasKeyName)
                .setSasKey(saskey);
       
        //exmaple of an endpoint for this evewnthub 
        //Endpoint=sb://<<hubName>>.servicebus.windows.net/;SharedAccessKeyName=<<sasKeyName>>;SharedAccessKey=<<sasKey>>
        final Gson gson = new GsonBuilder().create();

        // The Executor handles all asynchronous tasks and this is passed to the EventHubClient instance.
        // This enables the user to segregate their thread pool based on the work load.
        // This pool can then be shared across multiple EventHubClient instances.
        // The following sample uses a single thread executor, as there is only one EventHubClient instance,
        // handling different flavors of ingestion to Event Hubs here.
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

        // Each EventHubClient instance spins up a new TCP/SSL connection, which is expensive.
        // It is always a best practice to reuse these instances. The following sample shows this.
        final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);


        try {
            for (int i = 0; i < 10; i++) {

                String payload = "Message " + Integer.toString(i);
                byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
                EventData sendEvent = EventData.create(payloadBytes);

                // Send - not tied to any partition
                // Event Hubs service will round-robin the events across all Event Hubs partitions.
                // This is the recommended & most reliable way to send to Event Hubs.
                ehClient.sendSync(sendEvent);
            }

            System.out.println(Instant.now() + ": Send Complete...");
            System.out.println("Press Enter to stop.");
            System.in.read();
        } finally {
            ehClient.closeSync();
            executorService.shutdown();
        }
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("hello world");
		Driver d = new Driver();
		try {
			d.connect(System.getenv("AZ_NAMESPACE"), 
                                  System.getenv("AZ_HUBNAME"), 
                                  System.getenv("AZ_SASKEYNAME"),
                                  System.getenv("AZ_SASKEY"));
		} catch(Exception e) {
			System.out.println("failure: " + e.getMessage());
		}
	}

}
