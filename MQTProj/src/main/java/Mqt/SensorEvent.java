package Mqt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SensorEvent {
    private final String name;
    private final String city;
    private final String color;

    public SensorEvent(
            @JsonProperty("name") String name,
            @JsonProperty("city") String city,
            @JsonProperty("color") String color
    ) {
        this.name = name;
        this.city = city;
        this.color = color;
    }

    public String getName() {
        return name;
    }

    public String getCity() {
        return city;
    }

    public String getColor() {
        return color;
    }

    public String toJSONString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            e.printStackTrace();
            return "Error creating on toJSONString";
        }
    }
}