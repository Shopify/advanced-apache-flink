package dev.irontools.flink.serde.json;

import com.alibaba.fastjson2.JSONFactory;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.reader.ObjectReader;
import com.alibaba.fastjson2.reader.ObjectReaderProvider;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

/**
 * Deserialization schema that deserializes from JSON format FAST.
 *
 * @param <T> type of record it produces
 */
public class FastJsonDeserializationSchema<T> extends AbstractDeserializationSchema<T> {

  private static final long serialVersionUID = 1;

  private final Class<T> clazz;

  private transient ObjectReader<T> objectReader;
  private transient JSONReader.Context jsonContext;

  /**
   * Creates {@link FastJsonDeserializationSchema} that produces a record using provided class.
   *
   * @param clazz class of record to be produced
   */
  public FastJsonDeserializationSchema(Class<T> clazz) {
    super(clazz);
    this.clazz = clazz;
  }

  @Override
  public void open(InitializationContext context) {
    ObjectReaderProvider provider = JSONFactory.getDefaultObjectReaderProvider();
    jsonContext = new JSONReader.Context(provider);
    objectReader = provider.getObjectReader(clazz, false);
  }

  @Override
  public T deserialize(byte[] message) throws IOException {
    if (message == null || message.length == 0) {
      return null;
    }

    try (JSONReader reader = JSONReader.of(message, jsonContext)) {
      return objectReader.readObject(reader, clazz, null, 0);
    }
  }
}
