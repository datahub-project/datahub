package io.datahubproject.test.wiring;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;

/**
 * Records which Spring configuration classes and {@code @Bean} methods a test run really executed,
 * for the wiring ratchet ({@code checkWiringCoverage}, see {@code gradle/wiring/README.md}).
 *
 * <p>Test contexts built by the Spring TestContext framework ({@code @ContextConfiguration}, {@code
 * AbstractTestNGSpringContextTests}, {@code @SpringBootTest}) and by {@code SpringApplication} are
 * recorded automatically through {@code META-INF/spring.factories}. Contexts a test builds by hand
 * must opt in:
 *
 * <pre>{@code
 * new ApplicationContextRunner().withInitializer(WiringCoverage.initializer())...
 * AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
 * WiringCoverage.attach(ctx);
 * }</pre>
 *
 * <p>Lines written to {@code $datahub.wiring.coverage.dir/<random>.txt}:
 *
 * <ul>
 *   <li>{@code C <class>}: a configuration class (full or lite) was instantiated as a bean.
 *   <li>{@code M <class>#<method>}: a {@code @Bean} factory method was invoked and produced a bean
 *       that is not a Mockito mock. Beans replaced with {@code @MockitoBean} never reach the
 *       factory method, so they are not recorded.
 * </ul>
 *
 * <p>Without the system property (e.g. a test run from an IDE) everything here is a no-op.
 */
public final class WiringCoverage {

  public static final String DIR_PROPERTY = "datahub.wiring.coverage.dir";

  private static final String CONFIGURATION_CLASS_ATTRIBUTE =
      "org.springframework.context.annotation.ConfigurationClassPostProcessor.configurationClass";

  private static final Set<String> SEEN = ConcurrentHashMap.newKeySet();
  private static final Path OUTPUT = resolveOutput();

  private WiringCoverage() {}

  /** Whether this JVM records wiring coverage (the Gradle {@code test} task sets the property). */
  public static boolean enabled() {
    return OUTPUT != null;
  }

  /** Registers the recorder on a context that has not been refreshed yet. Idempotent. */
  public static void attach(ConfigurableApplicationContext context) {
    if (!enabled()) {
      return;
    }
    ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
    if (beanFactory instanceof org.springframework.beans.factory.support.AbstractBeanFactory abf
        && abf.getBeanPostProcessors().stream().anyMatch(Recorder.class::isInstance)) {
      return;
    }
    beanFactory.addBeanPostProcessor(new Recorder(beanFactory));
  }

  /** For {@code ApplicationContextRunner#withInitializer} and similar builders. */
  public static ApplicationContextInitializer<ConfigurableApplicationContext> initializer() {
    return WiringCoverage::attach;
  }

  private static Path resolveOutput() {
    String dir = System.getProperty(DIR_PROPERTY);
    if (dir == null || dir.isBlank()) {
      return null;
    }
    return Paths.get(dir, "recorded-" + UUID.randomUUID() + ".txt");
  }

  static void record(String line) {
    if (OUTPUT == null || !SEEN.add(line)) {
      return;
    }
    synchronized (WiringCoverage.class) {
      try {
        Files.createDirectories(OUTPUT.getParent());
        Files.writeString(
            OUTPUT,
            line + "\n",
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot write wiring coverage to " + OUTPUT, e);
      }
    }
  }

  private static final Method MOCKING_DETAILS = mockingDetailsMethod();

  private static Method mockingDetailsMethod() {
    try {
      return Class.forName("org.mockito.Mockito").getMethod("mockingDetails", Object.class);
    } catch (ReflectiveOperationException | LinkageError e) {
      return null;
    }
  }

  private static boolean isMock(Object bean) {
    if (MOCKING_DETAILS == null) {
      return false;
    }
    try {
      Object details = MOCKING_DETAILS.invoke(null, bean);
      return (boolean) details.getClass().getMethod("isMock").invoke(details);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return false;
    }
  }

  /** Sees every bean the context creates after it is registered. */
  static final class Recorder implements BeanPostProcessor {
    private final ConfigurableListableBeanFactory beanFactory;

    Recorder(ConfigurableListableBeanFactory beanFactory) {
      this.beanFactory = beanFactory;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName)
        throws BeansException {
      try {
        recordBean(bean, beanName);
      } catch (RuntimeException e) {
        // Never let coverage bookkeeping break a test context.
      }
      return bean;
    }

    private void recordBean(Object bean, String beanName) {
      if (beanName == null || !beanFactory.containsBeanDefinition(beanName)) {
        return;
      }
      BeanDefinition definition = beanFactory.getMergedBeanDefinition(beanName);
      Class<?> userClass = ClassUtils.getUserClass(bean);
      if (definition.getAttribute(CONFIGURATION_CLASS_ATTRIBUTE) != null
          || AnnotatedElementUtils.hasAnnotation(userClass, Configuration.class)) {
        record("C " + userClass.getName());
      }
      if (definition instanceof RootBeanDefinition root) {
        Method factoryMethod = root.getResolvedFactoryMethod();
        if (factoryMethod != null && !isMock(bean)) {
          Class<?> declaring = ClassUtils.getUserClass(factoryMethod.getDeclaringClass());
          record("M " + declaring.getName() + "#" + factoryMethod.getName());
        }
      }
    }
  }
}
