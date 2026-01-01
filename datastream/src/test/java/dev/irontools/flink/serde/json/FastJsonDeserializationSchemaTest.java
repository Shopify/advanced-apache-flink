package dev.irontools.flink.serde.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class FastJsonDeserializationSchemaTest {

  @Test
  public void testPojo() throws IOException {
    FastJsonDeserializationSchema<dev.irontools.flink.Customer> fastDeserializationSchema =
        new FastJsonDeserializationSchema<>(dev.irontools.flink.Customer.class);
    fastDeserializationSchema.open(null);

    byte[] data =
        "{\"id\":1234538243,\"first_name\":\"John\",\"last_name\":\"Doe\",\"address\":\"123 Main St\",\"country\":\"USA\",\"status\":\"active\",\"balance\":100000000}"
            .getBytes(StandardCharsets.UTF_8);

    dev.irontools.flink.Customer deserialized = fastDeserializationSchema.deserialize(data);
    assertEquals(1234538243, deserialized.id);
    assertEquals("John", deserialized.first_name);
    assertEquals("123 Main St", deserialized.address);
    assertEquals("USA", deserialized.country);
    assertEquals("active", deserialized.status);
    assertEquals(100000000, deserialized.balance);
  }
}
