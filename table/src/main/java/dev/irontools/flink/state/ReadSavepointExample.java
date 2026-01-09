package dev.irontools.flink.state;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ReadSavepointExample {
  public static void main(String[] args) {
      EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
      TableEnvironment tEnv = TableEnvironment.create(settings);

      tEnv.executeSql("LOAD MODULE state");
      tEnv.executeSql("SELECT * FROM savepoint_metadata('/tmp/flink-savepoints/savepoint-a95dbd-c082a8e380a0')").print();
    }
}
