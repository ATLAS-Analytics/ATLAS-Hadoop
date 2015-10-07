package edu.uchicago.networkweather;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class PacketLossInterceptor implements Interceptor {
	private static final Logger LOG = LoggerFactory.getLogger(PacketLossInterceptor.class);

	public PacketLossInterceptor() {
	}

	public void initialize() {
	}

	public void configure(Context context) {
	}

	public List<Event> intercepts(Event event) {

		JsonParser parser = new JsonParser();
		final Charset charset = Charset.forName("UTF-8");

		// This is the event's body
		String body = new String(event.getBody());
		// LOG.debug(body);
		
		JsonObject jBody;
		try {
			jBody = parser.parse(body).getAsJsonObject();
		} catch (JsonSyntaxException e) {
			LOG.error("problem in parsing msg body");
			return null;
		}

		// Map<String, String> newheaders = new HashMap<String, String>(1);
		String source = jBody.get("meta").getAsJsonObject().get("source").toString();
		String destination = jBody.get("meta").getAsJsonObject().get("destination").toString();
		String body1 = "{\"source\":\"" + source + "\",\"destination\":\"" + destination + "\",\"packet_loss\":";

		// LOG.debug(source);
		// LOG.debug(destination);

		JsonArray summaries = jBody.get("summaries").getAsJsonArray();

		JsonArray results = null;
		for (int ind = 0; ind < summaries.size(); ind++) {
			JsonObject sum = summaries.get(ind).getAsJsonObject();
			if (sum.get("summary_window").getAsString().equalsIgnoreCase("300")) {
				results = sum.get("summary_data").getAsJsonArray();
				break;
			}
		}
		// LOG.debug(results.toString());

		List<Event> measurements = new ArrayList<Event>(results.size());

		for (int ind = 0; ind < results.size(); ind++) {
			Long ts = results.get(ind).getAsJsonArray().get(0).getAsLong() * 1000;
			// newheaders.put("timestamp", ts.toString());
			Float packetLoss = results.get(ind).getAsJsonArray().get(1).getAsFloat();
			String bod = body1 + packetLoss.toString() + ",\"timestamp\":" + ts.toString() + "}";
			Event evnt = EventBuilder.withBody(bod, charset);// , newheaders);
			// LOG.debug(evnt.toString());
			measurements.add(evnt);
		}

		return measurements;
	}

	public Event intercept(Event event) {
		// LOG.debug("SINGLE EVENT PROCESSING");
		// JsonParser parser = new JsonParser();
		//
		// // This is the event's body
		// String body = new String(event.getBody());
		// LOG.debug(body);
		// JsonObject jBody;
		// try {
		// jBody = parser.parse(body).getAsJsonObject();
		// } catch (JsonSyntaxException e) {
		// LOG.error("problem in parsing msg body");
		// return null;
		// }
		// String source=
		// jBody.get("meta").getAsJsonObject().get("source").toString();
		// String destination=
		// jBody.get("meta").getAsJsonObject().get("destination").toString();
		// JsonArray summaries = jBody.get("summaries").getAsJsonArray();
		//
		// LOG.debug(source);
		// LOG.debug(destination);
		// JsonArray results=null;
		// for (int ind = 0; ind < summaries.size(); ind++) {
		// JsonObject sum=summaries.get(ind).getAsJsonObject();
		// if (sum.get("summary_window").getAsString().equalsIgnoreCase("300")){
		// results=sum.get("summary_data").getAsJsonArray();
		// break;
		// }
		// }
		// LOG.debug(results.toString());
		//
		// // These are the event's headers
		// Map<String, String> headers = event.getHeaders();
		//
		// // Enrich header with timestamp?
		// headers.put("derived", "123.123");

		// Let the enriched event go
		return event;
	}

	public List<Event> intercept(List<Event> events) {

		List<Event> interceptedEvents = new ArrayList<Event>(events.size());
		for (Event event : events) {
			interceptedEvents.addAll(intercepts(event));
		}
		LOG.info("got a list of " + events.size() + " events. Returned " + interceptedEvents.size() + " measurements.");
		return interceptedEvents;
	}

	public void close() {
		return;
	}

	public static class Builder implements Interceptor.Builder {

		public void configure(Context context) {
			// Retrieve property from flume conf
			// hostHeader = context.getString("hostHeader");
		}

		public Interceptor build() {
			return new PacketLossInterceptor();
		}
	}

}