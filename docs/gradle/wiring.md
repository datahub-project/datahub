# Wiring ratchet

Every Spring configuration class in a Java module (a class annotated with `@Configuration`,
`@SpringBootApplication`, `@AutoConfiguration`, ..., or any concrete class that declares `@Bean`
methods) must be **loaded by a real-context test in the same module**, or be listed in that
module's `wiring-baseline.txt`. The baseline may only shrink: the check fails on entries that are
no longer needed, and adding an entry is a reviewed exception (the build cannot tell a new entry
from an old one).

A wiring bug (a missing `@Import`, a wrong `@Conditional`, a constructor that throws with
production config) only shows up when Spring actually runs the `@Bean` method. Unit tests that
call `new FooFactory().createFoo(...)`, and Spring tests that replace the bean with
`@MockitoBean`, never do.

## How it works

1. **Recording.** Every Java module except `datahub-frontend` (Play/Guice) gets `:wiring-coverage`
   on its test classpath. Through
   `META-INF/spring.factories` it attaches a `BeanPostProcessor` to every context built by the
   Spring TestContext framework (`@ContextConfiguration`, `AbstractTestNGSpringContextTests`,
   `@SpringBootTest`) and by `SpringApplication`. It writes to
   `build/wiring-coverage/test/` which configuration classes were instantiated and which `@Bean`
   methods really ran. Beans replaced with `@MockitoBean` never reach their `@Bean` method, so
   they do not count.
2. **Checking.** `test` is finalized by `checkWiringCoverage`, which scans the module's compiled
   main classes and compares them against the recording and `wiring-baseline.txt`.

A configuration class is **covered** when at least one of its own (not inherited) `@Bean` methods
ran for real in a test context, or, for a class with no `@Bean` methods, when a test context
instantiated it. The report
in `build/reports/wiring-coverage/report.txt` also lists `@Bean` methods of covered classes that
no test ran.

Coverage is per module: a factory in `metadata-service/factories` loaded only by a
`metadata-service/war` test does not count. This keeps the check deterministic when CI shards
tests by module.

The check is skipped when the test task failed or ran with `--tests` (a partial run). It adds a
class-file scan (well under a second) to each module's test run.

A module with no test sources has nothing to record, so its check runs from `check` without a test
run, and every configuration class it has must be baselined. Custom annotations meta-annotated with
`@Configuration` are only recognised when declared in the same module; classes using one from
another module are found only if they declare `@Bean` methods.

## When the check fails

- **New uncovered class:** add a Spring test that loads it and lets at least one of its `@Bean`
  methods run (or, if it has none, that instantiates it), mocking its _dependencies_ rather than
  its beans. Contexts built by hand must opt in:

  ```java
  new ApplicationContextRunner()
      .withInitializer(WiringCoverage.initializer())
      .withUserConfiguration(MyFactory.class)
      ...

  AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
  WiringCoverage.attach(ctx);
  ```

  Adding the class to `wiring-baseline.txt` is a last resort, for reviewers to judge.

- **Baselined class is now covered** (or was deleted): remove it from `wiring-baseline.txt`, or
  run `./gradlew :module:test -PupdateWiringBaseline`, which only prunes.
