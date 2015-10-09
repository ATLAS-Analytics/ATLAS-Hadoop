package edu.uchicago.networkweather;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class OwLatencyInterceptor implements Interceptor {
	private static final Logger LOG = LoggerFactory.getLogger(OwLatencyInterceptor.class);

	private static JsonParser parser = new JsonParser();
	final Charset charset = Charset.forName("UTF-8");

	public OwLatencyInterceptor() {
	}

	public void initialize() {
	}

	public void configure(Context context) {
	}

	public List<Event> intercepts(Event event) {

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

		if (!jBody.has("meta")) {
			LOG.warn("this event has no meta.");
			return null;
		}

		String source, destination;

		try {
			source = jBody.get("meta").getAsJsonObject().get("source").toString();
			destination = jBody.get("meta").getAsJsonObject().get("destination").toString();
		} catch (Exception e) {
			LOG.warn("problem in parsing meta info." + e.toString());
			return null;
		}

		String body1 = "{\"src\":" + source + ",\"dest\":" + destination + ",\"delay_mean\":";

		Map<String, String> newheaders = new HashMap<String, String>(1);

		if (!jBody.has("summaries")) {
			LOG.warn("this event has no summaries of any kind.");
			return null;
		}

		JsonArray summaries = jBody.get("summaries").getAsJsonArray();

		JsonArray results = null;
		try {
			for (int ind = 0; ind < summaries.size(); ind++) {
				JsonObject sum = summaries.get(ind).getAsJsonObject();
				if (sum.get("summary_window").getAsString().equalsIgnoreCase("300") && sum.get("summary_type").getAsString().equalsIgnoreCase("statistics")) {
					results = sum.get("summary_data").getAsJsonArray();
					break;
				}
			}
		} catch (Exception e) {
			LOG.warn("problem in getting summary info." + e.toString());
		}

		if (results.size() == 0) {
			LOG.warn("message has no summary in 5 min intervals.");
			return null;
		} else {
			// LOG.debug("results:" + results.toString());
		}

		List<Event> measurements = new ArrayList<Event>(results.size());

		for (int ind = 0; ind < results.size(); ind++) {
			try {
				Long ts = results.get(ind).getAsJsonArray().get(0).getAsLong() * 1000;
				newheaders.put("timestamp", ts.toString());
			} catch (Exception e) {
				LOG.warn("problem in parsing timestamp info." + e.toString());
			}

			Float mean = 0F;
			Float median = 0F;
			Float stdev = 0F;
			JsonObject stat = results.get(ind).getAsJsonArray().get(1).getAsJsonObject();
			try {
				mean = stat.get("mean").getAsFloat();
				median = stat.get("median").getAsFloat();
				stdev = stat.get("standard-deviation").getAsFloat();
			} catch (UnsupportedOperationException e) {
				LOG.debug("problem in parsing statistics info." + e.getMessage() );
				e.printStackTrace();
				LOG.debug(stat.toString());
			}

			String bod = body1 + mean.toString() + ",\"delay_median\":" + median.toString() + ",\"delay_sd\":" + stdev.toString() + "}";

			LOG.debug(bod);
			Event evnt = EventBuilder.withBody(bod.getBytes(charset), newheaders);
			// LOG.debug(evnt.toString());
			measurements.add(evnt);
		}
		return measurements;
	}

	public Event intercept(Event event) {
		LOG.warn("SINGLE EVENT PROCESSING");
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		LOG.info("got a list of " + events.size() + " events");
		List<Event> interceptedEvents = new ArrayList<Event>(events.size());
		for (Event event : events) {
			List<Event> remadeEvents = intercepts(event);
			if (remadeEvents != null) {
				interceptedEvents.addAll(remadeEvents);
			}
		}
		LOG.info("Returned " + interceptedEvents.size() + " measurements.\n");
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
			return new OwLatencyInterceptor();
		}
	}

}