package security;

import static auth.sso.oidc.OidcConfigs.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.OidcProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.ConfigMergeable;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.pac4j.oidc.client.OidcClient;

public class OidcConfigurationTest {

  private static final com.typesafe.config.Config CONFIG =
      new Config() {

        private final Map<String, Object> _map = new HashMap<>();

        @Override
        public ConfigObject root() {
          return null;
        }

        @Override
        public ConfigOrigin origin() {
          return null;
        }

        @Override
        public Config withFallback(ConfigMergeable other) {
          return null;
        }

        @Override
        public Config resolve() {
          return null;
        }

        @Override
        public Config resolve(ConfigResolveOptions options) {
          return null;
        }

        @Override
        public boolean isResolved() {
          return false;
        }

        @Override
        public Config resolveWith(Config source) {
          return null;
        }

        @Override
        public Config resolveWith(Config source, ConfigResolveOptions options) {
          return null;
        }

        @Override
        public void checkValid(Config reference, String... restrictToPaths) {}

        @Override
        public boolean hasPath(String path) {
          return true;
        }

        @Override
        public boolean hasPathOrNull(String path) {
          return false;
        }

        @Override
        public boolean isEmpty() {
          return false;
        }

        @Override
        public Set<Map.Entry<String, ConfigValue>> entrySet() {
          return null;
        }

        @Override
        public boolean getIsNull(String path) {
          return false;
        }

        @Override
        public boolean getBoolean(String path) {
          return false;
        }

        @Override
        public Number getNumber(String path) {
          return null;
        }

        @Override
        public int getInt(String path) {
          return 0;
        }

        @Override
        public long getLong(String path) {
          return 0;
        }

        @Override
        public double getDouble(String path) {
          return 0;
        }

        @Override
        public String getString(String path) {
          return (String) _map.getOrDefault(path, "1");
        }

        @Override
        public <T extends Enum<T>> T getEnum(Class<T> enumClass, String path) {
          return null;
        }

        @Override
        public ConfigObject getObject(String path) {
          return null;
        }

        @Override
        public Config getConfig(String path) {
          return null;
        }

        @Override
        public Object getAnyRef(String path) {
          return null;
        }

        @Override
        public ConfigValue getValue(String path) {
          return null;
        }

        @Override
        public Long getBytes(String path) {
          return null;
        }

        @Override
        public ConfigMemorySize getMemorySize(String path) {
          return null;
        }

        @Override
        public Long getMilliseconds(String path) {
          return null;
        }

        @Override
        public Long getNanoseconds(String path) {
          return null;
        }

        @Override
        public long getDuration(String path, TimeUnit unit) {
          return 0;
        }

        @Override
        public Duration getDuration(String path) {
          return null;
        }

        @Override
        public Period getPeriod(String path) {
          return null;
        }

        @Override
        public TemporalAmount getTemporal(String path) {
          return null;
        }

        @Override
        public ConfigList getList(String path) {
          return null;
        }

        @Override
        public List<Boolean> getBooleanList(String path) {
          return null;
        }

        @Override
        public List<Number> getNumberList(String path) {
          return null;
        }

        @Override
        public List<Integer> getIntList(String path) {
          return null;
        }

        @Override
        public List<Long> getLongList(String path) {
          return null;
        }

        @Override
        public List<Double> getDoubleList(String path) {
          return null;
        }

        @Override
        public List<String> getStringList(String path) {
          return null;
        }

        @Override
        public <T extends Enum<T>> List<T> getEnumList(Class<T> enumClass, String path) {
          return null;
        }

        @Override
        public List<? extends ConfigObject> getObjectList(String path) {
          return null;
        }

        @Override
        public List<? extends Config> getConfigList(String path) {
          return null;
        }

        @Override
        public List<? extends Object> getAnyRefList(String path) {
          return null;
        }

        @Override
        public List<Long> getBytesList(String path) {
          return null;
        }

        @Override
        public List<ConfigMemorySize> getMemorySizeList(String path) {
          return null;
        }

        @Override
        public List<Long> getMillisecondsList(String path) {
          return null;
        }

        @Override
        public List<Long> getNanosecondsList(String path) {
          return null;
        }

        @Override
        public List<Long> getDurationList(String path, TimeUnit unit) {
          return null;
        }

        @Override
        public List<Duration> getDurationList(String path) {
          return null;
        }

        @Override
        public Config withOnlyPath(String path) {
          return null;
        }

        @Override
        public Config withoutPath(String path) {
          return null;
        }

        @Override
        public Config atPath(String path) {
          return null;
        }

        @Override
        public Config atKey(String key) {
          return null;
        }

        @Override
        public Config withValue(String path, ConfigValue value) {
          _map.put(path, value.unwrapped());
          return this;
        }
      };

  @Test
  public void readTimeoutPropagation() {

    CONFIG.withValue(OIDC_READ_TIMEOUT, ConfigValueFactory.fromAnyRef("10000"));
    OidcConfigs oidcConfigs = new OidcConfigs(CONFIG);
    OidcProvider oidcProvider = new OidcProvider(oidcConfigs);
    assertEquals(10000, ((OidcClient) oidcProvider.client()).getConfiguration().getReadTimeout());
  }
}
