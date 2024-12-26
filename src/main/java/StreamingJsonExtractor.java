import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StreamingJsonExtractor {
    private final JsonParser parser;
    private long cnt = 0;
    private long matches = 0;

    public StreamingJsonExtractor(JsonParser parser) {
        this.parser = parser;
    }

    private JsonToken nextToken() throws IOException {
        cnt++;
        if (cnt % 1_000_000 == 0) {
            System.out.println(cnt / 1_000_000 + " M token");
        }
        if (cnt == 100) {
            //System.exit(0);
        }
        return parser.nextToken();
    }

    private Object parse(String parent, boolean store) throws IOException {
        //System.err.println(parent + " " + store);
        var token = parent != null ? parser.currentToken() : nextToken();
        if (token == JsonToken.START_ARRAY) {
            var list = new ArrayList<>();
            while (nextToken() != JsonToken.END_ARRAY) {
                var obj = parse(parent, store);
                if (store) {
                    list.add(obj);
                }
            }
            return list;
        } else if (token == JsonToken.START_OBJECT) {
            Map<String, Object> obj = new HashMap<>();
            store |= "in_network".equals(parent);
            while (nextToken() != JsonToken.END_OBJECT) {
                // field name
                nextToken();
                var fieldName = parser.currentName();
                // field value
                var fieldValue = parse(fieldName, store);
                if (store) {
                    obj.put(fieldName, fieldValue);
                }
            }
            if ("CPT".equals(obj.get("billing_code_type"))) {
                //System.err.println(obj);
                matches++;
            }
            return obj;
        } else if (token.isScalarValue()) {
            if (token == JsonToken.VALUE_TRUE) {
                return true;
            }
            if (token == JsonToken.VALUE_FALSE) {
                return false;
            }
            if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                return parser.getDoubleValue();
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                return parser.getLongValue();
            }
            return parser.getValueAsString();
        } else {
            throw new IllegalStateException(token.toString());
        }
    }

    public static void main(String[] args) throws IOException {
        File jsonFile = new File("./FloridaBlue_GBO_in-network-rates.json");
        JsonFactory factory = new JsonFactory();
        long before = System.currentTimeMillis();
        try (JsonParser parser = factory.createParser(jsonFile)) {
            var p = new StreamingJsonExtractor(parser);
            p.parse(null, false);
            System.err.println(p.cnt + " tokens total, " + p.matches + " matches");
        }
        System.err.println((System.currentTimeMillis() - before) / 1000 + " seconds");
    }
}
